package org.davidmoten.rx.jdbc;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

public class Util {
    
    public static void closeAll(ResultSet rs) {
        Statement stmt = null;
        try {
            stmt = rs.getStatement();
        } catch (SQLException e) {
            // ignore
        }
        try {
            rs.close();
        } catch (SQLException e) {
            // ignore
        }
        if (stmt != null) {
            closeAll(stmt);
        }

    }

    public static void closeAll(Statement stmt) {
        try {
            stmt.close();
        } catch (SQLException e) {
            // ignore
        }
        Connection con = null;
        try {
            con = stmt.getConnection();
        } catch (SQLException e1) {
            // ignore
        }
        if (con != null) {
            try {
                con.close();
            } catch (SQLException e) {
                // ignore
            }
        }
    }
    
     static PreparedStatement setParameters(PreparedStatement ps, List<Object> parameters) {
		//TODO
		return ps;
	}

}
