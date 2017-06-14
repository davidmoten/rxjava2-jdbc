package org.davidmoten.rx.jdbc.pool;

public final class Pools {
    
    private Pools() {
        //prevent instantiation
    }
    
    public static NonBlockingConnectionPool2.Builder nonBlocking() {
        return NonBlockingConnectionPool2.builder();
    }
}
