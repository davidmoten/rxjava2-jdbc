package org.davidmoten.rx.jdbc.pool;

import java.sql.Connection;

import org.davidmoten.rx.jdbc.pool.internal.HealthCheckPredicate;

import io.reactivex.functions.Predicate;

public enum DatabaseType {

    ORACLE("select 1 from dual"), //
    HSQLDB("SELECT 1 FROM INFORMATION_SCHEMA.SYSTEM_USERS"), //
    H2("select 1"), //
    SQL_SERVER("select 1"), //
    MYSQL("select 1"), //
    POSTGRES("select 1"), //
    SQLITE("select 1"), //
    DB2("select 1 from sysibm.sysdummy1"), //
    DERBY("SELECT 1 FROM SYSIBM.SYSDUMMY1"), //
    INFORMIX("select count(*) from systables"), //
    OTHER("select 1");

    private final String healthCheckSql;

    private DatabaseType(String healthCheckSql) {
        this.healthCheckSql = healthCheckSql;
    }

    public Predicate<Connection >healthCheck() {
        return new HealthCheckPredicate(healthCheckSql);
    }
}
