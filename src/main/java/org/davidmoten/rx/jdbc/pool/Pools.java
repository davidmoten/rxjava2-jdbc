package org.davidmoten.rx.jdbc.pool;

public final class Pools {
    
    private Pools() {
        //prevent instantiation
    }
    
    public static NonBlockingConnectionPool.Builder nonBlocking() {
        return NonBlockingConnectionPool.builder();
    }
}
