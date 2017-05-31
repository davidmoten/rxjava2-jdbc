package org.davidmoten.rx.jdbc.exceptions;

import org.davidmoten.rx.jdbc.exceptions.SQLRuntimeException;

public final class QueryAnnotationMissingException extends SQLRuntimeException {

    private static final long serialVersionUID = 6725264065633977594L;

    public QueryAnnotationMissingException(String message) {
        super(message);
    }
}
