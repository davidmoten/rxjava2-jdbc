package org.davidmoten.rx.jdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class StoredProcExample {

    public static void zero() {
        // do nothing
    }

    public static void in1out1(int a, int[] b) {
        b[0] = a;
    }

    public static void in1out2(int a, int[] b, int c[]) {
        b[0] = a;
        c[0] = a + 1;
    }

    public static void in1out3(int a, int[] b, int c[], int d[]) {
        b[0] = a;
        c[0] = a + 1;
        d[0] = a + 2;
    }

    public static void out10(int[] a, int[] b, int[] c, int[] d, int[] e, int[] f, int[] g, int[] h, int[] i, int[] j) {
        a[0] = 1;
        b[0] = 2;
        c[0] = 3;
        d[0] = 4;
        e[0] = 5;
        f[0] = 6;
        g[0] = 7;
        h[0] = 8;
        i[0] = 9;
        j[0] = 10;
    }

    public static void rs10(ResultSet[] a, ResultSet[] b, ResultSet[] c, ResultSet[] d, ResultSet[] e, ResultSet[] f,
            ResultSet[] g, ResultSet[] h, ResultSet[] i, ResultSet[] j) throws SQLException {
        try (Connection con = DriverManager.getConnection("jdbc:default:connection")) {
            setRs1(con, a);
            setRs2(con, b);
            setRs1(con, c);
            setRs2(con, d);
            setRs1(con, e);
            setRs2(con, f);
            setRs1(con, g);
            setRs2(con, h);
            setRs1(con, i);
            setRs2(con, j);
        }
    }

    public static void getPersonCount(int minScore, int[] count) throws SQLException {
        try (Connection con = DriverManager.getConnection("jdbc:default:connection");
                PreparedStatement stmt = prepareStatement(con, minScore);
                ResultSet rs = stmt.executeQuery()) {
            rs.next();
            count[0] = rs.getInt(1);
            System.out.println("returning getPersonCount=" + count[0]);
        }
    }

    private static PreparedStatement prepareStatement(Connection con, int minScore) throws SQLException {
        PreparedStatement stmt = con.prepareStatement("select count(*) from app.person where score>?");
        stmt.setInt(1, minScore);
        return stmt;
    }

    public static void in0out0rs1(ResultSet[] rs1) throws SQLException {
        try (Connection con = DriverManager.getConnection("jdbc:default:connection")) {
            // don't close the statement!
            PreparedStatement stmt = con.prepareStatement("select name, score from person order by name");
            rs1[0] = stmt.executeQuery();
        }
    }

    public static void in1out0rs2(int minScore, ResultSet[] rs1, ResultSet[] rs2) throws SQLException {
        try (Connection con = DriverManager.getConnection("jdbc:default:connection")) {
            // don't close the statement!
            {
                PreparedStatement stmt = con
                        .prepareStatement("select name, score from person where score >= ? order by name");
                stmt.setInt(1, minScore);
                rs1[0] = stmt.executeQuery();
            }
            {
                PreparedStatement stmt = con
                        .prepareStatement("select name, score from person where score >= ? order by name desc");
                stmt.setInt(1, minScore);
                rs2[0] = stmt.executeQuery();
            }
        }
    }

    public static void in0out2rs3(int[] a, int[] b, ResultSet[] c, ResultSet[] d, ResultSet[] e) throws SQLException {
        a[0] = 1;
        b[0] = 2;
        try (Connection con = DriverManager.getConnection("jdbc:default:connection")) {
            // don't close the statement!
            setRs1(con, c);
            setRs2(con, d);
            setRs1(con, e);
        }
    }

    private static void setRs1(Connection con, ResultSet[] c) throws SQLException {
        PreparedStatement stmt = con.prepareStatement("select name, score from person order by name");
        c[0] = stmt.executeQuery();
    }

    private static void setRs2(Connection con, ResultSet[] c) throws SQLException {
        PreparedStatement stmt = con.prepareStatement("select name, score from person order by name desc");
        c[0] = stmt.executeQuery();
    }

    public static void in2out2(int a, int b, String[] name, int[] total) {
        name[0] = "FREDDY";
        total[0] = a + b;
    }

}
