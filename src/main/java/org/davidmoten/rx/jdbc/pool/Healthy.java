package org.davidmoten.rx.jdbc.pool;

import java.sql.Connection;
import java.sql.Statement;

import io.reactivex.functions.Predicate;

public final class Healthy {

    public final static Predicate<Connection> ORACLE = new HealthyPredicate("select 1 from dual");
    public final static Predicate<Connection> HSQLDB = new HealthyPredicate(
            "SELECT 1 FROM INFORMATION_SCHEMA.SYSTEM_USERS");
    public final static Predicate<Connection> OTHER =  new HealthyPredicate("select 1");
    public final static Predicate<Connection> H2 = OTHER;
    public final static Predicate<Connection> SQL_SERVER = OTHER;
    public final static Predicate<Connection> MYSQL = OTHER;
    public final static Predicate<Connection> POSTGRES = OTHER;
    public final static Predicate<Connection> SQLITE = OTHER;
    public final static Predicate<Connection> DB2 = new HealthyPredicate(
            "select 1 from sysibm.sysdummy1");
    public final static Predicate<Connection> DERBY = new HealthyPredicate(
            "SELECT 1 FROM SYSIBM.SYSDUMMY1");
    public final static Predicate<Connection> INFORMIX = new HealthyPredicate(
            "select count(*) from systables");

    private static final class HealthyPredicate implements Predicate<Connection> {

        private final String sql;

        HealthyPredicate(String sql) {
            this.sql = sql;
        }

        @Override
        public boolean test(Connection c) throws Exception {
            try (Statement s = c.createStatement()) {
                s.executeQuery(sql).close();
                return true;
            } catch (Throwable t) {
                return false;
            }
        }
    }
}
