package org.davidmoten.rx.jdbc.exceptions;

public final class MoreColumnsRequestedThanExistException extends SQLRuntimeException {

    private static final long serialVersionUID = -6120327049049973535L;
    
    public MoreColumnsRequestedThanExistException(String message) {
        super(message);
    }

}
