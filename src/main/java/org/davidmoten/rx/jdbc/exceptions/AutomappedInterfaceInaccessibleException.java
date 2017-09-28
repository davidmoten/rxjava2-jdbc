package org.davidmoten.rx.jdbc.exceptions;

public final class AutomappedInterfaceInaccessibleException extends SQLRuntimeException {

    private static final long serialVersionUID = -2315130441694532141L;

    public AutomappedInterfaceInaccessibleException(String msg) {
        super(msg);
    }

}
