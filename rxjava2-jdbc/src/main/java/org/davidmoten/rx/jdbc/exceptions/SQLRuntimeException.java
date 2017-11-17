package org.davidmoten.rx.jdbc.exceptions;

public class SQLRuntimeException extends RuntimeException {

    private static final long serialVersionUID = -3879393806890615797L;

    public SQLRuntimeException(Throwable e) {
        super(e);
    }

    public SQLRuntimeException(String message) {
        super(message);
    }

}
