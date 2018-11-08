package org.davidmoten.rx.jdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

import javax.annotation.Nonnull;

import org.davidmoten.rx.jdbc.exceptions.SQLRuntimeException;

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
     * connection pool may need formal closure to release its connections because
     * connection.close() is actually just releasing a connection back to the pool
     * for reuse. This method should be idempotent.
     */
    void close();

    /**
     * Warning: Don't pass one of these as a ConnectionProvider to a pool because
     * once the pool closes the connection the pool cannot create a new one (the
     * same closed connection is returned). Instead use a different ConnectionProvider
     * factory method.
     * 
     * @param connection connection for singleton provider
     * @return singleton connection provider (don't use with connection pools!)
     */
    static ConnectionProvider from(@Nonnull Connection connection) {
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

    static ConnectionProvider from(@Nonnull String url) {
        Preconditions.checkNotNull(url, "url cannot be null");
        return new ConnectionProvider() {

            @Override
            public Connection get() {
                try {
                    return DriverManager.getConnection(url);
                } catch (SQLException e) {
                    throw new SQLRuntimeException(e);
                }
            }

            @Override
            public void close() {
                // do nothing as closure will be handle by pool
            }
        };
    }

    static ConnectionProvider from(@Nonnull String url, String username, String password) {
        Preconditions.checkNotNull(url, "url cannot be null");
        return new ConnectionProvider() {

            @Override
            public Connection get() {
                try {
                    return DriverManager.getConnection(url, username, password);
                } catch (SQLException e) {
                    throw new SQLRuntimeException(e);
                }
            }

            @Override
            public void close() {
                // do nothing as closure will be handle by pool
            }
        };
    }
    
    static ConnectionProvider from(@Nonnull String url, Properties properties) {
        Preconditions.checkNotNull(url, "url cannot be null");
        Preconditions.checkNotNull(properties, "properties cannot be null");
        return new ConnectionProvider() {

            @Override
            public Connection get() {
                try {
                    return DriverManager.getConnection(url, properties);
                } catch (SQLException e) {
                    throw new SQLRuntimeException(e);
                }
            }

            @Override
            public void close() {
                // do nothing as closure will be handle by pool
            }
        };
    }

}
