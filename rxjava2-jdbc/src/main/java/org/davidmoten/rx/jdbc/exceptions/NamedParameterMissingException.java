package org.davidmoten.rx.jdbc.exceptions;

public final class NamedParameterMissingException extends SQLRuntimeException {

    private static final long serialVersionUID = -2218975686530672709L;

    public NamedParameterMissingException(String message) {
        super(message);
    }

}
