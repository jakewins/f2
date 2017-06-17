package com.jakewins.f2;

import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Set;

class DeadlockDescription {

    private F2ClientEntry[] chain;

    DeadlockDescription() {
        // Special case for NoDeadlockDescription
    }

    /**
     * @param chain describes the deadlock chain by client entries; the first entry would be the one our client is
     *              waiting for; all entries after that a chain of held entries
     */
    DeadlockDescription(F2ClientEntry ... chain) {
        assert assertIsValidDeadlockChain(chain);
        this.chain = chain;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        DeadlockDescription that = (DeadlockDescription) o;

        return Arrays.equals(chain, that.chain);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(chain);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("Deadlock[");
        for(int linkIndex=0;linkIndex<chain.length - 1;linkIndex++) {
            F2ClientEntry link = chain[linkIndex];
            sb.append(String.format("(%s)-[:WAITS_FOR]->(%s)->[:HELD_BY]->", link.owner, link.lock));
        }
        sb.append(String.format("(%s)", chain[chain.length-1].owner));
        return sb.append("]").toString();
    }

    private static boolean assertIsValidDeadlockChain(F2ClientEntry[] chain) {
        assert chain.length > 0 : String.format("Invalid deadlock chain: %s\n" +
                "Chain must be greater than zero", Arrays.toString(chain));


        F2ClientEntry lastHolder = null;
        for(int linkIndex=1;linkIndex<chain.length;linkIndex++) {
            F2ClientEntry waiter = chain[linkIndex - 1];
            F2ClientEntry holder = chain[linkIndex];

            // The waiter should be waiting on a lock held by the next waiter (eg. the holder)
            assertLockHeldBy(waiter.lock, holder.owner);

            // If the deadlock is triggering on a client waiting on itself, something is wrong
            assert waiter.owner != holder.owner : String.format("Invalid deadlock chain: %s\n" +
                    "%s and %s are same client.", Arrays.toString(chain), waiter, holder);

            // The waiter should be the same client as the holder from the previous iteration
            assert lastHolder == null || waiter.owner == lastHolder.owner : String.format("Invalid deadlock chain: %s\n" +
                    "LastHolder(%s) and Waiter(%s) are not the same client.", Arrays.toString(chain), lastHolder, waiter);

            // The waiter should be on the wait list
            boolean isOnWaitList = false;
            for(F2ClientEntry current = waiter.lock.waitList; current != null; current = current.next) {
                if(current == waiter) {
                    isOnWaitList = true;
                    break;
                }
            }
            assert isOnWaitList: String.format("Invalid deadlock chain: %s\n" +
                    "%s is not on wait list it claims to be on", Arrays.toString(chain), waiter);

            lastHolder = holder;
        }

        return true;
    }

    private static void assertLockHeldBy(F2Lock lock, F2Client expectedHolder) {
        if(lock.exclusiveHolder.owner != expectedHolder) {
            boolean isHoldingSharedLock = false;
            for(F2ClientEntry current = lock.sharedHolderList; current != null; current = current.next) {
                if(current.owner == expectedHolder) {
                    isHoldingSharedLock = true;
                    break;
                }
            }

            assert isHoldingSharedLock: String.format("Expected %s to hold shared or exclusive lock on %s. " +
                    "Exclusive holder is %s, shared holder HEAD is %s",
                    expectedHolder, lock, lock.exclusiveHolder, lock.sharedHolderList);
        }
    }
}

class NoDeadlockDescription extends DeadlockDescription {
    NoDeadlockDescription() {

    }

    public String toString() {
        return "No Deadlock";
    }

    @Override
    public int hashCode() {
        return 0;
    }

    @Override
    public boolean equals(Object obj) {
        return obj == this;
    }
}

class DeadlockDetector {
    static final DeadlockDescription NONE = new NoDeadlockDescription();

    /**
     * NOTE: It's assumed caller holds a stop-the-world lock on {@link F2Partitions}
     * @param source find loops from the perspective of this client
     * @return a description of any deadlock found, or {@link #NONE}
     */
    DeadlockDescription detectDeadlock(F2Client source) {
        LinkedList<F2ClientEntry> detectedDeadlockChain = new LinkedList<>();

        boolean foundDeadlock = detectRecursively(source, source, new HashSet<>(), detectedDeadlockChain, 0);
        if(!foundDeadlock) {
            return NONE;
        }

        // Describe the deadlock chain
        detectedDeadlockChain.push(source.waitsFor);
        F2ClientEntry[] chain = new F2ClientEntry[detectedDeadlockChain.size()];
        int chainIndex = 0;
        for(F2ClientEntry entry : detectedDeadlockChain) {
            chain[chainIndex++] = entry;
        }

        return new DeadlockDescription(chain);
    }

    private boolean detectRecursively(F2Client source, F2Client blockee, Set<F2Client> seen, LinkedList<F2ClientEntry> detectedDeadlockChain, int depth) {
        F2Lock lock;

        // Unblocked clients tell no tales
        if(blockee == null || blockee.waitsFor == null) {
            return false;
        }

        // If we've already explored this client, no need to do it again
        if(!seen.add(blockee)) {
            return false;
        }

        if(depth > 15) {
            throw new RuntimeException(String.format("source: %s, blockee: %s, waitsFor: %s", source, blockee, blockee.waitsFor));
        }

        lock = blockee.waitsFor.lock;

        // Is the lock we're (transitively) blocked on held exclusively?
        if(lock.exclusiveHolder != null) {
            // Is the blocking client waiting on us?
            if (lock.exclusiveHolder.owner == source) {
                detectedDeadlockChain.push(lock.exclusiveHolder.owner.waitsFor);
                return true;
            }

            if(lock.exclusiveHolder.owner == null) {
                throw new RuntimeException("Waitsfor record with owner == null: " + lock.exclusiveHolder);
            }
            if(lock.exclusiveHolder.owner == blockee) {
                throw new RuntimeException(String.format("Uhhh %s, waitsFor:%s source:%s", lock.exclusiveHolder, lock.exclusiveHolder.owner.waitsFor, source));
            }
            if (detectRecursively(source, lock.exclusiveHolder.owner, seen, detectedDeadlockChain, depth + 1)){
                // Found a loop
                detectedDeadlockChain.push(lock.exclusiveHolder.owner.waitsFor);
                return true;
            }
        }

        // If the entry we're blocked on is not waiting to get `lock` exclusively, then there's no reason
        // to check shared holders of that lock, because they do not block our blockee
        if(blockee.waitsFor.lockMode != LockMode.EXCLUSIVE) {
            return false;
        }

        for(F2ClientEntry current = lock.sharedHolderList; current != null; current = current.next) {
            if(current.owner == source) {
                // We have a share lock on the lock that our blocker wants an exclusive lock on; womp womp womp.
                detectedDeadlockChain.push(current.owner.waitsFor);
                return true;
            }

            if(detectRecursively(source, current.owner, seen, detectedDeadlockChain, depth + 1)) {
                // Found a loop
                detectedDeadlockChain.push(current.owner.waitsFor);
                return true;
            }
        }

        // Unable to find a loop
        return false;
    }
}
