package org.davidmoten.rx.jdbc.internal;

import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;

public interface DelegatedConnection extends Connection {

    Connection con();

    default <T> T unwrap(Class<T> iface) throws SQLException {
        return con().unwrap(iface);
    }

    default boolean isWrapperFor(Class<?> iface) throws SQLException {
        return con().isWrapperFor(iface);
    }

    default Statement createStatement() throws SQLException {
        return con().createStatement();
    }

    default PreparedStatement prepareStatement(String sql) throws SQLException {
        return con().prepareStatement(sql);
    }

    default CallableStatement prepareCall(String sql) throws SQLException {
        return con().prepareCall(sql);
    }

    default String nativeSQL(String sql) throws SQLException {
        return con().nativeSQL(sql);
    }

    default void setAutoCommit(boolean autoCommit) throws SQLException {
        con().setAutoCommit(autoCommit);
    }

    default boolean getAutoCommit() throws SQLException {
        return con().getAutoCommit();
    }

    default void commit() throws SQLException {
        con().commit();
    }

    default void rollback() throws SQLException {
        con().rollback();
    }

    default void close() throws SQLException {
        con().close();
    }

    default boolean isClosed() throws SQLException {
        return con().isClosed();
    }

    default DatabaseMetaData getMetaData() throws SQLException {
        return con().getMetaData();
    }

    default void setReadOnly(boolean readOnly) throws SQLException {
        con().setReadOnly(readOnly);
    }

    default boolean isReadOnly() throws SQLException {
        return con().isReadOnly();
    }

    default void setCatalog(String catalog) throws SQLException {
        con().setCatalog(catalog);
    }

    default String getCatalog() throws SQLException {
        return con().getCatalog();
    }

    default void setTransactionIsolation(int level) throws SQLException {
        con().setTransactionIsolation(level);
    }

    default int getTransactionIsolation() throws SQLException {
        return con().getTransactionIsolation();
    }

    default SQLWarning getWarnings() throws SQLException {
        return con().getWarnings();
    }

    default void clearWarnings() throws SQLException {
        con().clearWarnings();
    }

    default Statement createStatement(int resultSetType, int resultSetConcurrency)
            throws SQLException {
        return con().createStatement(resultSetType, resultSetConcurrency);
    }

    default PreparedStatement prepareStatement(String sql, int resultSetType,
            int resultSetConcurrency) throws SQLException {
        return con().prepareStatement(sql, resultSetType, resultSetConcurrency);
    }

    default CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency)
            throws SQLException {
        return con().prepareCall(sql, resultSetType, resultSetConcurrency);
    }

    default Map<String, Class<?>> getTypeMap() throws SQLException {
        return con().getTypeMap();
    }

    default void setTypeMap(Map<String, Class<?>> map) throws SQLException {
        con().setTypeMap(map);
    }

    default void setHoldability(int holdability) throws SQLException {
        con().setHoldability(holdability);
    }

    default int getHoldability() throws SQLException {
        return con().getHoldability();
    }

    default Savepoint setSavepoint() throws SQLException {
        return con().setSavepoint();
    }

    default Savepoint setSavepoint(String name) throws SQLException {
        return con().setSavepoint(name);
    }

    default void rollback(Savepoint savepoint) throws SQLException {
        con().rollback(savepoint);
    }

    default void releaseSavepoint(Savepoint savepoint) throws SQLException {
        con().releaseSavepoint(savepoint);
    }

    default Statement createStatement(int resultSetType, int resultSetConcurrency,
            int resultSetHoldability) throws SQLException {
        return con().createStatement(resultSetType, resultSetConcurrency, resultSetHoldability);
    }

    default PreparedStatement prepareStatement(String sql, int resultSetType,
            int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        return con().prepareStatement(sql, resultSetType, resultSetConcurrency,
                resultSetHoldability);
    }

    default CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency,
            int resultSetHoldability) throws SQLException {
        return con().prepareCall(sql, resultSetType, resultSetConcurrency, resultSetHoldability);
    }

    default PreparedStatement prepareStatement(String sql, int autoGeneratedKeys)
            throws SQLException {
        return con().prepareStatement(sql, autoGeneratedKeys);
    }

    default PreparedStatement prepareStatement(String sql, int[] columnIndexes)
            throws SQLException {
        return con().prepareStatement(sql, columnIndexes);
    }

    default PreparedStatement prepareStatement(String sql, String[] columnNames)
            throws SQLException {
        return con().prepareStatement(sql, columnNames);
    }

    default Clob createClob() throws SQLException {
        return con().createClob();
    }

    default Blob createBlob() throws SQLException {
        return con().createBlob();
    }

    default NClob createNClob() throws SQLException {
        return con().createNClob();
    }

    default SQLXML createSQLXML() throws SQLException {
        return con().createSQLXML();
    }

    default boolean isValid(int timeout) throws SQLException {
        return con().isValid(timeout);
    }

    default void setClientInfo(String name, String value) throws SQLClientInfoException {
        con().setClientInfo(name, value);
    }

    default void setClientInfo(Properties properties) throws SQLClientInfoException {
        con().setClientInfo(properties);
    }

    default String getClientInfo(String name) throws SQLException {
        return con().getClientInfo(name);
    }

    default Properties getClientInfo() throws SQLException {
        return con().getClientInfo();
    }

    default Array createArrayOf(String typeName, Object[] elements) throws SQLException {
        return con().createArrayOf(typeName, elements);
    }

    default Struct createStruct(String typeName, Object[] attributes) throws SQLException {
        return con().createStruct(typeName, attributes);
    }

    default void setSchema(String schema) throws SQLException {
        con().setSchema(schema);
    }

    default String getSchema() throws SQLException {
        return con().getSchema();
    }

    default void abort(Executor executor) throws SQLException {
        con().abort(executor);
    }

    default void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {
        con().setNetworkTimeout(executor, milliseconds);
    }

    default int getNetworkTimeout() throws SQLException {
        return con().getNetworkTimeout();
    }

}
