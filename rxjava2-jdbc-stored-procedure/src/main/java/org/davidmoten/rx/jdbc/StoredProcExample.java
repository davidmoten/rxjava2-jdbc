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
            System.out.println("returning getPersonCount="+ count[0]);
        }
    }

    private static PreparedStatement prepareStatement(Connection con, int minScore)
            throws SQLException {
        PreparedStatement stmt = con
                .prepareStatement("select count(*) from app.person where score>?");
        stmt.setInt(1, minScore);
        return stmt;
    }

    public static void returnResultSets(int minScore, ResultSet[] rs1, ResultSet[] rs2)
            throws SQLException {
        try (Connection con = DriverManager.getConnection("jdbc:default:connection")) {
            // don't close the statement!
            {
                PreparedStatement stmt = con.prepareStatement(
                        "select name, score from person where score >= ? order by name");
                stmt.setInt(1, minScore);
                rs1[0] = stmt.executeQuery();
            }
            {
                PreparedStatement stmt = con.prepareStatement(
                        "select name, score from person where score >= ? order by name desc");
                stmt.setInt(1, minScore);
                rs2[0] = stmt.executeQuery();
            }
        }
    }

    public static void twoInTwoOut(int a, int b, String[] name, int[] total) {
        name[0] = "FREDDY";
        total[0] = a + b;
    }

}
