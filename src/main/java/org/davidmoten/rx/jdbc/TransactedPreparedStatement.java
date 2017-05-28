package org.davidmoten.rx.jdbc;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.Ref;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;

public final class TransactedPreparedStatement implements PreparedStatement {

    private final TransactedConnection con;
    private final PreparedStatement ps;

    TransactedPreparedStatement(TransactedConnection con, PreparedStatement ps) {
        this.con = con;
        this.ps = ps;
    }

    @Override
    public void addBatch() throws SQLException {
        ps.addBatch();
    }

    @Override
    public void addBatch(String arg0) throws SQLException {
        ps.addBatch(arg0);
    }

    @Override
    public void cancel() throws SQLException {
        ps.cancel();
    }

    @Override
    public void clearBatch() throws SQLException {
        ps.clearBatch();
    }

    @Override
    public void clearParameters() throws SQLException {
        ps.clearParameters();
    }

    @Override
    public void clearWarnings() throws SQLException {
        ps.clearWarnings();
    }

    @Override
    public void close() throws SQLException {
        ps.close();
    }

    @Override
    public void closeOnCompletion() throws SQLException {
        ps.closeOnCompletion();
    }

    @Override
    public boolean execute() throws SQLException {
        return ps.execute();
    }

    @Override
    public boolean execute(String arg0, int arg1) throws SQLException {
        return ps.execute(arg0, arg1);
    }

    @Override
    public boolean execute(String arg0, int[] arg1) throws SQLException {
        return ps.execute(arg0, arg1);
    }

    @Override
    public boolean execute(String arg0, String[] arg1) throws SQLException {
        return ps.execute(arg0, arg1);
    }

    @Override
    public boolean execute(String arg0) throws SQLException {
        return ps.execute(arg0);
    }

    @Override
    public int[] executeBatch() throws SQLException {
        return ps.executeBatch();
    }

    @Override
    public TransactedResultSet executeQuery() throws SQLException {
        return new TransactedResultSet(this, ps.executeQuery());
    }

    @Override
    public TransactedResultSet executeQuery(String sql) throws SQLException {
        return new TransactedResultSet(this, ps.executeQuery(sql));
    }

    @Override
    public int executeUpdate() throws SQLException {
        return ps.executeUpdate();
    }

    @Override
    public int executeUpdate(String arg0, int arg1) throws SQLException {
        return ps.executeUpdate(arg0, arg1);
    }

    @Override
    public int executeUpdate(String arg0, int[] arg1) throws SQLException {
        return ps.executeUpdate(arg0, arg1);
    }

    @Override
    public int executeUpdate(String arg0, String[] arg1) throws SQLException {
        return ps.executeUpdate(arg0, arg1);
    }

    @Override
    public int executeUpdate(String arg0) throws SQLException {
        return ps.executeUpdate(arg0);
    }

    @Override
    public TransactedConnection getConnection() throws SQLException {
        return con;
    }

    @Override
    public int getFetchDirection() throws SQLException {
        return ps.getFetchDirection();
    }

    @Override
    public int getFetchSize() throws SQLException {
        return ps.getFetchSize();
    }

    @Override
    public TransactedResultSet getGeneratedKeys() throws SQLException {
        return new TransactedResultSet(this, ps.getGeneratedKeys());
    }

    @Override
    public int getMaxFieldSize() throws SQLException {
        return ps.getMaxFieldSize();
    }

    @Override
    public int getMaxRows() throws SQLException {
        return ps.getMaxRows();
    }

    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        return ps.getMetaData();
    }

    @Override
    public boolean getMoreResults() throws SQLException {
        return ps.getMoreResults();
    }

    @Override
    public boolean getMoreResults(int arg0) throws SQLException {
        return ps.getMoreResults(arg0);
    }

    @Override
    public ParameterMetaData getParameterMetaData() throws SQLException {
        return ps.getParameterMetaData();
    }

    @Override
    public int getQueryTimeout() throws SQLException {
        return ps.getQueryTimeout();
    }

    @Override
    public TransactedResultSet getResultSet() throws SQLException {
        return new TransactedResultSet(this, ps.getResultSet());
    }

    @Override
    public int getResultSetConcurrency() throws SQLException {
        return ps.getResultSetConcurrency();
    }

    @Override
    public int getResultSetHoldability() throws SQLException {
        return ps.getResultSetHoldability();
    }

    @Override
    public int getResultSetType() throws SQLException {
        return ps.getResultSetType();
    }

    @Override
    public int getUpdateCount() throws SQLException {
        return ps.getUpdateCount();
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        return ps.getWarnings();
    }

    @Override
    public boolean isCloseOnCompletion() throws SQLException {
        return ps.isCloseOnCompletion();
    }

    @Override
    public boolean isClosed() throws SQLException {
        return ps.isClosed();
    }

    @Override
    public boolean isPoolable() throws SQLException {
        return ps.isPoolable();
    }

    @Override
    public boolean isWrapperFor(Class<?> arg0) throws SQLException {
        return ps.isWrapperFor(arg0);
    }

    @Override
    public void setArray(int arg0, Array arg1) throws SQLException {
        ps.setArray(arg0, arg1);
    }

    @Override
    public void setAsciiStream(int arg0, InputStream arg1, int arg2) throws SQLException {
        ps.setAsciiStream(arg0, arg1, arg2);
    }

    @Override
    public void setAsciiStream(int arg0, InputStream arg1, long arg2) throws SQLException {
        ps.setAsciiStream(arg0, arg1, arg2);
    }

    @Override
    public void setAsciiStream(int arg0, InputStream arg1) throws SQLException {
        ps.setAsciiStream(arg0, arg1);
    }

    @Override
    public void setBigDecimal(int arg0, BigDecimal arg1) throws SQLException {
        ps.setBigDecimal(arg0, arg1);
    }

    @Override
    public void setBinaryStream(int arg0, InputStream arg1, int arg2) throws SQLException {
        ps.setBinaryStream(arg0, arg1, arg2);
    }

    @Override
    public void setBinaryStream(int arg0, InputStream arg1, long arg2) throws SQLException {
        ps.setBinaryStream(arg0, arg1, arg2);
    }

    @Override
    public void setBinaryStream(int arg0, InputStream arg1) throws SQLException {
        ps.setBinaryStream(arg0, arg1);
    }

    @Override
    public void setBlob(int arg0, Blob arg1) throws SQLException {
        ps.setBlob(arg0, arg1);
    }

    @Override
    public void setBlob(int arg0, InputStream arg1, long arg2) throws SQLException {
        ps.setBlob(arg0, arg1, arg2);
    }

    @Override
    public void setBlob(int arg0, InputStream arg1) throws SQLException {
        ps.setBlob(arg0, arg1);
    }

    @Override
    public void setBoolean(int arg0, boolean arg1) throws SQLException {
        ps.setBoolean(arg0, arg1);
    }

    @Override
    public void setByte(int arg0, byte arg1) throws SQLException {
        ps.setByte(arg0, arg1);
    }

    @Override
    public void setBytes(int arg0, byte[] arg1) throws SQLException {
        ps.setBytes(arg0, arg1);
    }

    @Override
    public void setCharacterStream(int arg0, Reader arg1, int arg2) throws SQLException {
        ps.setCharacterStream(arg0, arg1, arg2);
    }

    @Override
    public void setCharacterStream(int arg0, Reader arg1, long arg2) throws SQLException {
        ps.setCharacterStream(arg0, arg1, arg2);
    }

    @Override
    public void setCharacterStream(int arg0, Reader arg1) throws SQLException {
        ps.setCharacterStream(arg0, arg1);
    }

    @Override
    public void setClob(int arg0, Clob arg1) throws SQLException {
        ps.setClob(arg0, arg1);
    }

    @Override
    public void setClob(int arg0, Reader arg1, long arg2) throws SQLException {
        ps.setClob(arg0, arg1, arg2);
    }

    @Override
    public void setClob(int arg0, Reader arg1) throws SQLException {
        ps.setClob(arg0, arg1);
    }

    @Override
    public void setCursorName(String arg0) throws SQLException {
        ps.setCursorName(arg0);
    }

    @Override
    public void setDate(int arg0, Date arg1, Calendar arg2) throws SQLException {
        ps.setDate(arg0, arg1, arg2);
    }

    @Override
    public void setDate(int arg0, Date arg1) throws SQLException {
        ps.setDate(arg0, arg1);
    }

    @Override
    public void setDouble(int arg0, double arg1) throws SQLException {
        ps.setDouble(arg0, arg1);
    }

    @Override
    public void setEscapeProcessing(boolean arg0) throws SQLException {
        ps.setEscapeProcessing(arg0);
    }

    @Override
    public void setFetchDirection(int arg0) throws SQLException {
        ps.setFetchDirection(arg0);
    }

    @Override
    public void setFetchSize(int arg0) throws SQLException {
        ps.setFetchSize(arg0);
    }

    @Override
    public void setFloat(int arg0, float arg1) throws SQLException {
        ps.setFloat(arg0, arg1);
    }

    @Override
    public void setInt(int arg0, int arg1) throws SQLException {
        ps.setInt(arg0, arg1);
    }

    @Override
    public void setLong(int arg0, long arg1) throws SQLException {
        ps.setLong(arg0, arg1);
    }

    @Override
    public void setMaxFieldSize(int arg0) throws SQLException {
        ps.setMaxFieldSize(arg0);
    }

    @Override
    public void setMaxRows(int arg0) throws SQLException {
        ps.setMaxRows(arg0);
    }

    @Override
    public void setNCharacterStream(int arg0, Reader arg1, long arg2) throws SQLException {
        ps.setNCharacterStream(arg0, arg1, arg2);
    }

    @Override
    public void setNCharacterStream(int arg0, Reader arg1) throws SQLException {
        ps.setNCharacterStream(arg0, arg1);
    }

    @Override
    public void setNClob(int arg0, NClob arg1) throws SQLException {
        ps.setNClob(arg0, arg1);
    }

    @Override
    public void setNClob(int arg0, Reader arg1, long arg2) throws SQLException {
        ps.setNClob(arg0, arg1, arg2);
    }

    @Override
    public void setNClob(int arg0, Reader arg1) throws SQLException {
        ps.setNClob(arg0, arg1);
    }

    @Override
    public void setNString(int arg0, String arg1) throws SQLException {
        ps.setNString(arg0, arg1);
    }

    @Override
    public void setNull(int arg0, int arg1, String arg2) throws SQLException {
        ps.setNull(arg0, arg1, arg2);
    }

    @Override
    public void setNull(int arg0, int arg1) throws SQLException {
        ps.setNull(arg0, arg1);
    }

    @Override
    public void setObject(int arg0, Object arg1, int arg2, int arg3) throws SQLException {
        ps.setObject(arg0, arg1, arg2, arg3);
    }

    @Override
    public void setObject(int arg0, Object arg1, int arg2) throws SQLException {
        ps.setObject(arg0, arg1, arg2);
    }

    @Override
    public void setObject(int arg0, Object arg1) throws SQLException {
        ps.setObject(arg0, arg1);
    }

    @Override
    public void setPoolable(boolean arg0) throws SQLException {
        ps.setPoolable(arg0);
    }

    @Override
    public void setQueryTimeout(int arg0) throws SQLException {
        ps.setQueryTimeout(arg0);
    }

    @Override
    public void setRef(int arg0, Ref arg1) throws SQLException {
        ps.setRef(arg0, arg1);
    }

    @Override
    public void setRowId(int arg0, RowId arg1) throws SQLException {
        ps.setRowId(arg0, arg1);
    }

    @Override
    public void setSQLXML(int arg0, SQLXML arg1) throws SQLException {
        ps.setSQLXML(arg0, arg1);
    }

    @Override
    public void setShort(int arg0, short arg1) throws SQLException {
        ps.setShort(arg0, arg1);
    }

    @Override
    public void setString(int arg0, String arg1) throws SQLException {
        ps.setString(arg0, arg1);
    }

    @Override
    public void setTime(int arg0, Time arg1, Calendar arg2) throws SQLException {
        ps.setTime(arg0, arg1, arg2);
    }

    @Override
    public void setTime(int arg0, Time arg1) throws SQLException {
        ps.setTime(arg0, arg1);
    }

    @Override
    public void setTimestamp(int arg0, Timestamp arg1, Calendar arg2) throws SQLException {
        ps.setTimestamp(arg0, arg1, arg2);
    }

    @Override
    public void setTimestamp(int arg0, Timestamp arg1) throws SQLException {
        ps.setTimestamp(arg0, arg1);
    }

    @Override
    public void setURL(int arg0, URL arg1) throws SQLException {
        ps.setURL(arg0, arg1);
    }

    @Override
    @Deprecated
    public void setUnicodeStream(int arg0, InputStream arg1, int arg2) throws SQLException {
        ps.setUnicodeStream(arg0, arg1, arg2);
    }

    @Override
    public <T> T unwrap(Class<T> arg0) throws SQLException {
        return ps.unwrap(arg0);
    }

}
