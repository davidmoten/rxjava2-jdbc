package org.davidmoten.rx.jdbc.exceptions;

public final class AutomappedClassInaccessibleException extends SQLRuntimeException {

    private static final long serialVersionUID = -2315130441694532141L;

    public AutomappedClassInaccessibleException(String msg) {
        super(msg);
    }

}
