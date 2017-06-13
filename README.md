# F2

A concurrent lock manager with automatic deadlock detection.

## Overview

Good place to start reading is in [F2Client#acquire](src/main/java/com/jakewins/f2/F2Client.java).


The design is inspired by the lock manager in postgres; the locks are grouped into partitions, and each partition
is guarded by a stamped lock.

Acquiring a shared lock roughly consists of:

    # Entry to represent our relationship to the lock (held exclusively, held shared, on waitlist)
    lockEntry = newLockEntry()
    
    partition = partitions[resourceId % len(partitions)]
    
    patition.partitionLock.acquire()
    
    lock = partition.getLock(resourceId)  # Hashmap lookup
    
    if lock.exclusiveHolder == null:
        # If there is no exclusive holder, add a record for us in the shared holder list
        lockEntry.next = lock.sharedList
        lock.sharedList = lockEntry
        
        partition.partitionLock.release()
        return
        
    # If there is a shared holder, add ourselves to wait list and wait to be signalled
    lockEntry.next = lock.waitList
    lock.waitList = lockEntry
    
    partition.partitionLock.release()
    waitForSignal()
    return
 
 
## Performance

Way too early to tell, but there are some interesting indications. There's a JMH microbenchmark for testing some of the
primitives and comparing them to Forseti. For acquiring/releasing a single shared lock with CPUS * 4 threads:

    Benchmark                                        Mode  Cnt     Score      Error   Units
    F2Locks_PerfTest.f2AcquireContendedShared       thrpt    5  6995.419 ±  809.099  ops/ms
    F2Locks_PerfTest.forsetiAcquireContendedShared  thrpt    5  5496.916 ± 1980.028  ops/ms
    
There's a *very* interesting note from running this; since Forseti is doing a spin/backoff wait, while F2 is waiting for
a notify to get back to work, F2 uses almost no CPU.

Under a real world load, someone holding a lock can be expected to want to use (some) CPU; having waiters eat that CPU
up is no good, so the F2 approach could have some good outcomes.

Trying to benchmark exclusive locks highlighted a bug in the deadlock detector (infinite recursion..), so that'll have
to wait until tomorrow.