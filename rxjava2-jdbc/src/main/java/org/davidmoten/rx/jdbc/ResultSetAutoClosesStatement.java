package org.davidmoten.rx.jdbc;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.davidmoten.rx.jdbc.internal.DelegatedResultSet;

class ResultSetAutoClosesStatement extends DelegatedResultSet {

    private final PreparedStatement ps;

    ResultSetAutoClosesStatement(ResultSet rs, PreparedStatement ps) {
        super(rs);
        this.ps = ps;
    }

    @Override
    public void close() throws SQLException {
        Throwable e1 = null;
        try {
            rs.close();
        } catch (Throwable e) {
            e1 = e;
        }
        Throwable e2 = null;
        try {
            ps.close();
        } catch (Throwable e) {
            e2 = e;
        }
        if (e1 != null) {
            throw new SQLException(e1);
        } else if (e2 != null) {
            throw new SQLException(e2);
        }
    }

}
