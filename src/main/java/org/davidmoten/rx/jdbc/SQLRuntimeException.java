package org.davidmoten.rx.jdbc;

import java.sql.SQLException;

public class SQLRuntimeException extends RuntimeException {

    private static final long serialVersionUID = -3879393806890615797L;

    public SQLRuntimeException(SQLException e) {
        super(e);
    }
}
