package org.davidmoten.rx.jdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class StoredProcExample {

    public static void getPersonCount(int minScore, int[] count) throws SQLException {
        try (Connection con = DriverManager.getConnection("jdbc:default:connection");
                PreparedStatement stmt = prepareStatement(con, minScore);
                ResultSet rs = stmt.executeQuery()) {
            rs.next();
            count[0] = rs.getInt(1);
        }
    }

    private static PreparedStatement prepareStatement(Connection con, int minScore)
            throws SQLException {
        PreparedStatement stmt = con.prepareStatement("select count(*) from app.person where score>?");
        stmt.setInt(1, minScore);
        return stmt;
    }

}
