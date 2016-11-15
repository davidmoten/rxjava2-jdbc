package org.davidmoten.rx.jdbc;

public final class ParameterMissingNameException extends SQLRuntimeException {

    private static final long serialVersionUID = -604688060878761249L;

    public ParameterMissingNameException(String message) {
        super(message);
    }

}
