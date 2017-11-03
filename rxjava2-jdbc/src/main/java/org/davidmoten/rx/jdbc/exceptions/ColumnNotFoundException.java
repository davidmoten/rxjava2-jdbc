package org.davidmoten.rx.jdbc.exceptions;

public final class ColumnNotFoundException extends SQLRuntimeException{

    private static final long serialVersionUID = 2344698645797692551L;

    public ColumnNotFoundException(String message) {
        super(message);
    }
}
