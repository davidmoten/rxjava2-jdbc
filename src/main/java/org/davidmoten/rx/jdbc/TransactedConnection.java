package org.davidmoten.rx.jdbc;

import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.NClob;
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
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class TransactedConnection implements Connection {

    private static final Logger log = LoggerFactory.getLogger(TransactedConnection.class);

    private final Connection con;
    private final AtomicInteger counter;

    TransactedConnection(Connection con, AtomicInteger counter) {
        log.debug("constructing TransactedConnection from {}, {}", con, counter);
        this.con = con;
        this.counter = counter;
    }

    public TransactedConnection(Connection con) {
        this(con, new AtomicInteger(1));
    }

    public int counter() {
        return counter.get();
    }

    @Override
    public void abort(Executor executor) throws SQLException {
        con.abort(executor);
    }

    @Override
    public void clearWarnings() throws SQLException {
        con.clearWarnings();
    }

    public TransactedConnection fork() {
        if (counter.getAndIncrement() > 0) {
            return new TransactedConnection(con, counter);
        } else {
            throw new RuntimeException("cannot fork TransactedConnection because already closed");
        }
    }

    @Override
    public void close() throws SQLException {
        log.debug("TransactedConnection attempt close");
        if (counter.get() == 0) {
            log.debug("TransactedConnection close");
            con.close();
        }
    }

    @Override
    public void commit() throws SQLException {
        log.debug("TransactedConnection commit attempt, counter={}", counter.get());
        if (counter.decrementAndGet() == 0) {
            log.debug("TransactedConnection actual commit");
            con.commit();
        }
    }

    @Override
    public void rollback() throws SQLException {
        log.debug("TransactedConnection rollback attempt, counter={}", counter.get());
        if (counter.decrementAndGet() == 0) {
            log.debug("TransactedConnection actual rollback");
            con.rollback();
        }
    }

    @Override
    public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
        return con.createArrayOf(typeName, elements);
    }

    @Override
    public Blob createBlob() throws SQLException {
        return con.createBlob();
    }

    @Override
    public Clob createClob() throws SQLException {
        return con.createClob();
    }

    @Override
    public NClob createNClob() throws SQLException {
        return con.createNClob();
    }

    @Override
    public SQLXML createSQLXML() throws SQLException {
        return con.createSQLXML();
    }

    @Override
    public Statement createStatement() throws SQLException {
        return con.createStatement();
    }

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency,
            int resultSetHoldability) throws SQLException {
        return con.createStatement(resultSetType, resultSetConcurrency, resultSetHoldability);
    }

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency)
            throws SQLException {
        return con.createStatement(resultSetType, resultSetConcurrency);
    }

    @Override
    public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
        return con.createStruct(typeName, attributes);
    }

    @Override
    public boolean getAutoCommit() throws SQLException {
        return con.getAutoCommit();
    }

    @Override
    public String getCatalog() throws SQLException {
        return con.getCatalog();
    }

    @Override
    public Properties getClientInfo() throws SQLException {
        return con.getClientInfo();
    }

    @Override
    public String getClientInfo(String name) throws SQLException {
        return con.getClientInfo(name);
    }

    @Override
    public int getHoldability() throws SQLException {
        return con.getHoldability();
    }

    @Override
    public DatabaseMetaData getMetaData() throws SQLException {
        return con.getMetaData();
    }

    @Override
    public int getNetworkTimeout() throws SQLException {
        return con.getNetworkTimeout();
    }

    @Override
    public String getSchema() throws SQLException {
        return con.getSchema();
    }

    @Override
    public int getTransactionIsolation() throws SQLException {
        return con.getTransactionIsolation();
    }

    @Override
    public Map<String, Class<?>> getTypeMap() throws SQLException {
        return con.getTypeMap();
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        return con.getWarnings();
    }

    @Override
    public boolean isClosed() throws SQLException {
        return con.isClosed();
    }

    @Override
    public boolean isReadOnly() throws SQLException {
        return con.isReadOnly();
    }

    @Override
    public boolean isValid(int timeout) throws SQLException {
        return con.isValid(timeout);
    }

    @Override
    public boolean isWrapperFor(Class<?> arg0) throws SQLException {
        return con.isWrapperFor(arg0);
    }

    @Override
    public String nativeSQL(String sql) throws SQLException {
        return con.nativeSQL(sql);
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency,
            int resultSetHoldability) throws SQLException {
        return con.prepareCall(sql, resultSetType, resultSetConcurrency, resultSetHoldability);
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency)
            throws SQLException {
        return con.prepareCall(sql, resultSetType, resultSetConcurrency);
    }

    @Override
    public CallableStatement prepareCall(String sql) throws SQLException {
        return con.prepareCall(sql);
    }

    @Override
    public TransactedPreparedStatement prepareStatement(String sql, int resultSetType,
            int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        return new TransactedPreparedStatement(this, con.prepareStatement(sql, resultSetType,
                resultSetConcurrency, resultSetHoldability));
    }

    @Override
    public TransactedPreparedStatement prepareStatement(String sql, int resultSetType,
            int resultSetConcurrency) throws SQLException {
        return new TransactedPreparedStatement(this,
                con.prepareStatement(sql, resultSetType, resultSetConcurrency));
    }

    @Override
    public TransactedPreparedStatement prepareStatement(String sql, int autoGeneratedKeys)
            throws SQLException {
        return new TransactedPreparedStatement(this, con.prepareStatement(sql, autoGeneratedKeys));
    }

    @Override
    public TransactedPreparedStatement prepareStatement(String sql, int[] columnIndexes)
            throws SQLException {
        return new TransactedPreparedStatement(this, con.prepareStatement(sql, columnIndexes));
    }

    @Override
    public TransactedPreparedStatement prepareStatement(String sql, String[] columnNames)
            throws SQLException {
        return new TransactedPreparedStatement(this, con.prepareStatement(sql, columnNames));
    }

    @Override
    public TransactedPreparedStatement prepareStatement(String sql) throws SQLException {
        return new TransactedPreparedStatement(this, con.prepareStatement(sql));
    }

    @Override
    public void releaseSavepoint(Savepoint savepoint) throws SQLException {
        con.releaseSavepoint(savepoint);
    }

    @Override
    public void rollback(Savepoint savepoint) throws SQLException {
        con.rollback(savepoint);
    }

    @Override
    public void setAutoCommit(boolean autoCommit) throws SQLException {
        con.setAutoCommit(autoCommit);
    }

    @Override
    public void setCatalog(String catalog) throws SQLException {
        con.setCatalog(catalog);
    }

    @Override
    public void setClientInfo(Properties properties) throws SQLClientInfoException {
        con.setClientInfo(properties);
    }

    @Override
    public void setClientInfo(String name, String value) throws SQLClientInfoException {
        con.setClientInfo(name, value);
    }

    @Override
    public void setHoldability(int holdability) throws SQLException {
        con.setHoldability(holdability);
    }

    @Override
    public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {
        con.setNetworkTimeout(executor, milliseconds);
    }

    @Override
    public void setReadOnly(boolean readOnly) throws SQLException {
        con.setReadOnly(readOnly);
    }

    @Override
    public Savepoint setSavepoint() throws SQLException {
        return con.setSavepoint();
    }

    @Override
    public Savepoint setSavepoint(String name) throws SQLException {
        return con.setSavepoint(name);
    }

    @Override
    public void setSchema(String schema) throws SQLException {
        con.setSchema(schema);
    }

    @Override
    public void setTransactionIsolation(int level) throws SQLException {
        con.setTransactionIsolation(level);
    }

    @Override
    public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
        con.setTypeMap(map);
    }

    @Override
    public <T> T unwrap(Class<T> arg0) throws SQLException {
        return con.unwrap(arg0);
    }

    public void incrementCounter() {
        counter.incrementAndGet();
    }
    
    public void decrementCounter() {
        counter.decrementAndGet();
    }

    @Override
    public String toString() {
        return "TransactedConnection [con=" + con + ", counter=" + counter + "]";
    }

}
