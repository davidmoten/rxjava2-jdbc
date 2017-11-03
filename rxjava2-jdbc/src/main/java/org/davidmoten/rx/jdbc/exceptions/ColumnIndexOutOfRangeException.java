package org.davidmoten.rx.jdbc.exceptions;

public final class ColumnIndexOutOfRangeException extends SQLRuntimeException {

    private static final long serialVersionUID = 900639264983427617L;

    public ColumnIndexOutOfRangeException(String message) {
        super (message);
    }
    
}
