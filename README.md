# F2

A concurrent lock manager with automatic deadlock detection.

## Overview

Good place to start reading is in [F2Client#acquire](src/main/java/com/jakewins/f2/F2Client).


The design is inspired by the lock manager in postgres; the locks are grouped into partitions, and each partition
is guarded by a stamped lock.

In the best case, acquiring an open shared lock roughly consists of:

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
 