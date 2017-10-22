package org.davidmoten.rx.jdbc.pool;

import java.sql.Connection;

import org.davidmoten.rx.jdbc.pool.internal.HealthCheckPredicate;

import io.reactivex.functions.Predicate;

public final class HealthCheck {

    public final static Predicate<Connection> ORACLE = new HealthCheckPredicate("select 1 from dual");
    public final static Predicate<Connection> HSQLDB = new HealthCheckPredicate(
            "SELECT 1 FROM INFORMATION_SCHEMA.SYSTEM_USERS");
    public final static Predicate<Connection> OTHER = new HealthCheckPredicate("select 1");
    public final static Predicate<Connection> H2 = OTHER;
    public final static Predicate<Connection> SQL_SERVER = OTHER;
    public final static Predicate<Connection> MYSQL = OTHER;
    public final static Predicate<Connection> POSTGRES = OTHER;
    public final static Predicate<Connection> SQLITE = OTHER;
    public final static Predicate<Connection> DB2 = new HealthCheckPredicate(
            "select 1 from sysibm.sysdummy1");
    public final static Predicate<Connection> DERBY = new HealthCheckPredicate(
            "SELECT 1 FROM SYSIBM.SYSDUMMY1");
    public final static Predicate<Connection> INFORMIX = new HealthCheckPredicate(
            "select count(*) from systables");
}
