package com.jakewins.f2.include;

/** Locks are split by resource types. It is up to the implementation to define the contract for these. */
public interface ResourceType
{
    /** Must be unique among all existing resource types, should preferably be a sequence starting at 0. */
    int typeId();

    /** What to do if the lock cannot immediately be acquired. */
    WaitStrategy waitStrategy();

    /** Must be unique among all existing resource types. */
    String name();
}