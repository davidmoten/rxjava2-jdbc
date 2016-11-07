package org.davidmoten.rx.jdbc;

public class DisposerShouldNotThrowException extends RuntimeException{

    private static final long serialVersionUID = -7562281601391362666L;

    public DisposerShouldNotThrowException(Throwable e) {
        super(e);
    }
    
}
