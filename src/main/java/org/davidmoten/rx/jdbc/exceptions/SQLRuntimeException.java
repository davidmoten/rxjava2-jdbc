package org.davidmoten.rx.jdbc.exceptions;

import java.sql.SQLException;

public class SQLRuntimeException extends RuntimeException {

    private static final long serialVersionUID = -3879393806890615797L;

    public SQLRuntimeException(SQLException e) {
        super(e);
    }

    public SQLRuntimeException() {
        super();
    }

    public SQLRuntimeException(String message) {
        super(message);
    }

    public SQLRuntimeException(Exception e) {
        super(e);
    }
}
