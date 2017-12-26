package org.davidmoten.rx.jdbc.exceptions;

public final class CannotForkTransactedConnection extends SQLRuntimeException {

    private static final long serialVersionUID = -5632849589133364479L;

    public CannotForkTransactedConnection(String message) {
        super(message);
    }

}
