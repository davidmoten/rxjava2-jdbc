package org.davidmoten.rx.jdbc.exceptions;

public final class MethodCallNotSupportedException extends SQLRuntimeException {

    private static final long serialVersionUID = 8350628640286671305L;

    public MethodCallNotSupportedException(String message) {
        super(message);
    }

}
