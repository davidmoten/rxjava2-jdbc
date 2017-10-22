package org.davidmoten.rx.jdbc.pool.internal;

import java.sql.Connection;
import java.sql.Statement;

import io.reactivex.functions.Predicate;

public final class HealthCheckPredicate implements Predicate<Connection> {

        private final String sql;

        public HealthCheckPredicate(String sql) {
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
