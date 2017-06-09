package com.jakewins.f2.include;


/** What to do if we need to wait. */
public interface WaitStrategy<EXCEPTION extends Exception>
{
    /**
     * Throws Exception to force users of this interface to handle any possible failure, since this is used in
     * potentially very sensitive code.
     */
    void apply( long iteration ) throws EXCEPTION;
}