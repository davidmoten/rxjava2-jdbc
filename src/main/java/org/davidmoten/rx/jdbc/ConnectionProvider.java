package org.davidmoten.rx.jdbc;

import java.sql.Connection;

import javax.annotation.Nonnull;

import com.github.davidmoten.guavamini.Preconditions;

/**
 * Provides JDBC Connections as required. It is advisable generally to use a
 * Connection pool.
 * 
 **/
public interface ConnectionProvider {

    /**
     * Returns a new {@link Connection} (perhaps from a Connection pool).
     * 
     * @return a new Connection to a database
     */
    @Nonnull 
    Connection get();

    /**
     * Closes the connection provider and releases its resources. For example, a
     * connection pool may need formal closure to release its connections
     * because connection.close() is actually just releasing a connection back
     * to the pool for reuse. This method should be idempotent.
     */
    void close();

    public static ConnectionProvider from(@Nonnull Connection connection) {
        Preconditions.checkNotNull(connection, "connection cannot be null");
        return new ConnectionProvider() {

            @Override
            public Connection get() {
                return connection;
            }

            @Override
            public void close() {
                // do nothing as con was not created by this provider
            }
        };
    }

}
