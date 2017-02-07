package org.davidmoten.rx.jdbc;

public final class DatabaseException extends RuntimeException {

    private static final long serialVersionUID = -4485320161715756372L;

    public DatabaseException(Throwable e) {
        super (e);
    }
    
}
