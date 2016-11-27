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

public class TransactedPreparedStatement implements PreparedStatement {

    private final TransactedConnection con;
    private final PreparedStatement ps;

    public TransactedPreparedStatement(TransactedConnection con, PreparedStatement ps) {
        this.con = con;
        this.ps = ps;
    }

    public void addBatch() throws SQLException {
        ps.addBatch();
    }

    public void addBatch(String arg0) throws SQLException {
        ps.addBatch(arg0);
    }

    public void cancel() throws SQLException {
        ps.cancel();
    }

    public void clearBatch() throws SQLException {
        ps.clearBatch();
    }

    public void clearParameters() throws SQLException {
        ps.clearParameters();
    }

    public void clearWarnings() throws SQLException {
        ps.clearWarnings();
    }

    public void close() throws SQLException {
        ps.close();
    }

    public void closeOnCompletion() throws SQLException {
        ps.closeOnCompletion();
    }

    public boolean execute() throws SQLException {
        return ps.execute();
    }

    public boolean execute(String arg0, int arg1) throws SQLException {
        return ps.execute(arg0, arg1);
    }

    public boolean execute(String arg0, int[] arg1) throws SQLException {
        return ps.execute(arg0, arg1);
    }

    public boolean execute(String arg0, String[] arg1) throws SQLException {
        return ps.execute(arg0, arg1);
    }

    public boolean execute(String arg0) throws SQLException {
        return ps.execute(arg0);
    }

    public int[] executeBatch() throws SQLException {
        return ps.executeBatch();
    }

    public TransactedResultSet executeQuery() throws SQLException {
        return new TransactedResultSet(this, ps.executeQuery());
    }

    public TransactedResultSet executeQuery(String sql) throws SQLException {
        return new TransactedResultSet(this, ps.executeQuery(sql));
    }

    public int executeUpdate() throws SQLException {
        return ps.executeUpdate();
    }

    public int executeUpdate(String arg0, int arg1) throws SQLException {
        return ps.executeUpdate(arg0, arg1);
    }

    public int executeUpdate(String arg0, int[] arg1) throws SQLException {
        return ps.executeUpdate(arg0, arg1);
    }

    public int executeUpdate(String arg0, String[] arg1) throws SQLException {
        return ps.executeUpdate(arg0, arg1);
    }

    public int executeUpdate(String arg0) throws SQLException {
        return ps.executeUpdate(arg0);
    }

    public TransactedConnection getConnection() throws SQLException {
        return con;
    }

    public int getFetchDirection() throws SQLException {
        return ps.getFetchDirection();
    }

    public int getFetchSize() throws SQLException {
        return ps.getFetchSize();
    }

    public TransactedResultSet getGeneratedKeys() throws SQLException {
        return new TransactedResultSet(this, ps.getGeneratedKeys());
    }

    public int getMaxFieldSize() throws SQLException {
        return ps.getMaxFieldSize();
    }

    public int getMaxRows() throws SQLException {
        return ps.getMaxRows();
    }

    public ResultSetMetaData getMetaData() throws SQLException {
        return ps.getMetaData();
    }

    public boolean getMoreResults() throws SQLException {
        return ps.getMoreResults();
    }

    public boolean getMoreResults(int arg0) throws SQLException {
        return ps.getMoreResults(arg0);
    }

    public ParameterMetaData getParameterMetaData() throws SQLException {
        return ps.getParameterMetaData();
    }

    public int getQueryTimeout() throws SQLException {
        return ps.getQueryTimeout();
    }

    public TransactedResultSet getResultSet() throws SQLException {
        return new TransactedResultSet(this, ps.getResultSet());
    }

    public int getResultSetConcurrency() throws SQLException {
        return ps.getResultSetConcurrency();
    }

    public int getResultSetHoldability() throws SQLException {
        return ps.getResultSetHoldability();
    }

    public int getResultSetType() throws SQLException {
        return ps.getResultSetType();
    }

    public int getUpdateCount() throws SQLException {
        return ps.getUpdateCount();
    }

    public SQLWarning getWarnings() throws SQLException {
        return ps.getWarnings();
    }

    public boolean isCloseOnCompletion() throws SQLException {
        return ps.isCloseOnCompletion();
    }

    public boolean isClosed() throws SQLException {
        return ps.isClosed();
    }

    public boolean isPoolable() throws SQLException {
        return ps.isPoolable();
    }

    public boolean isWrapperFor(Class<?> arg0) throws SQLException {
        return ps.isWrapperFor(arg0);
    }

    public void setArray(int arg0, Array arg1) throws SQLException {
        ps.setArray(arg0, arg1);
    }

    public void setAsciiStream(int arg0, InputStream arg1, int arg2) throws SQLException {
        ps.setAsciiStream(arg0, arg1, arg2);
    }

    public void setAsciiStream(int arg0, InputStream arg1, long arg2) throws SQLException {
        ps.setAsciiStream(arg0, arg1, arg2);
    }

    public void setAsciiStream(int arg0, InputStream arg1) throws SQLException {
        ps.setAsciiStream(arg0, arg1);
    }

    public void setBigDecimal(int arg0, BigDecimal arg1) throws SQLException {
        ps.setBigDecimal(arg0, arg1);
    }

    public void setBinaryStream(int arg0, InputStream arg1, int arg2) throws SQLException {
        ps.setBinaryStream(arg0, arg1, arg2);
    }

    public void setBinaryStream(int arg0, InputStream arg1, long arg2) throws SQLException {
        ps.setBinaryStream(arg0, arg1, arg2);
    }

    public void setBinaryStream(int arg0, InputStream arg1) throws SQLException {
        ps.setBinaryStream(arg0, arg1);
    }

    public void setBlob(int arg0, Blob arg1) throws SQLException {
        ps.setBlob(arg0, arg1);
    }

    public void setBlob(int arg0, InputStream arg1, long arg2) throws SQLException {
        ps.setBlob(arg0, arg1, arg2);
    }

    public void setBlob(int arg0, InputStream arg1) throws SQLException {
        ps.setBlob(arg0, arg1);
    }

    public void setBoolean(int arg0, boolean arg1) throws SQLException {
        ps.setBoolean(arg0, arg1);
    }

    public void setByte(int arg0, byte arg1) throws SQLException {
        ps.setByte(arg0, arg1);
    }

    public void setBytes(int arg0, byte[] arg1) throws SQLException {
        ps.setBytes(arg0, arg1);
    }

    public void setCharacterStream(int arg0, Reader arg1, int arg2) throws SQLException {
        ps.setCharacterStream(arg0, arg1, arg2);
    }

    public void setCharacterStream(int arg0, Reader arg1, long arg2) throws SQLException {
        ps.setCharacterStream(arg0, arg1, arg2);
    }

    public void setCharacterStream(int arg0, Reader arg1) throws SQLException {
        ps.setCharacterStream(arg0, arg1);
    }

    public void setClob(int arg0, Clob arg1) throws SQLException {
        ps.setClob(arg0, arg1);
    }

    public void setClob(int arg0, Reader arg1, long arg2) throws SQLException {
        ps.setClob(arg0, arg1, arg2);
    }

    public void setClob(int arg0, Reader arg1) throws SQLException {
        ps.setClob(arg0, arg1);
    }

    public void setCursorName(String arg0) throws SQLException {
        ps.setCursorName(arg0);
    }

    public void setDate(int arg0, Date arg1, Calendar arg2) throws SQLException {
        ps.setDate(arg0, arg1, arg2);
    }

    public void setDate(int arg0, Date arg1) throws SQLException {
        ps.setDate(arg0, arg1);
    }

    public void setDouble(int arg0, double arg1) throws SQLException {
        ps.setDouble(arg0, arg1);
    }

    public void setEscapeProcessing(boolean arg0) throws SQLException {
        ps.setEscapeProcessing(arg0);
    }

    public void setFetchDirection(int arg0) throws SQLException {
        ps.setFetchDirection(arg0);
    }

    public void setFetchSize(int arg0) throws SQLException {
        ps.setFetchSize(arg0);
    }

    public void setFloat(int arg0, float arg1) throws SQLException {
        ps.setFloat(arg0, arg1);
    }

    public void setInt(int arg0, int arg1) throws SQLException {
        ps.setInt(arg0, arg1);
    }

    public void setLong(int arg0, long arg1) throws SQLException {
        ps.setLong(arg0, arg1);
    }

    public void setMaxFieldSize(int arg0) throws SQLException {
        ps.setMaxFieldSize(arg0);
    }

    public void setMaxRows(int arg0) throws SQLException {
        ps.setMaxRows(arg0);
    }

    public void setNCharacterStream(int arg0, Reader arg1, long arg2) throws SQLException {
        ps.setNCharacterStream(arg0, arg1, arg2);
    }

    public void setNCharacterStream(int arg0, Reader arg1) throws SQLException {
        ps.setNCharacterStream(arg0, arg1);
    }

    public void setNClob(int arg0, NClob arg1) throws SQLException {
        ps.setNClob(arg0, arg1);
    }

    public void setNClob(int arg0, Reader arg1, long arg2) throws SQLException {
        ps.setNClob(arg0, arg1, arg2);
    }

    public void setNClob(int arg0, Reader arg1) throws SQLException {
        ps.setNClob(arg0, arg1);
    }

    public void setNString(int arg0, String arg1) throws SQLException {
        ps.setNString(arg0, arg1);
    }

    public void setNull(int arg0, int arg1, String arg2) throws SQLException {
        ps.setNull(arg0, arg1, arg2);
    }

    public void setNull(int arg0, int arg1) throws SQLException {
        ps.setNull(arg0, arg1);
    }

    public void setObject(int arg0, Object arg1, int arg2, int arg3) throws SQLException {
        ps.setObject(arg0, arg1, arg2, arg3);
    }

    public void setObject(int arg0, Object arg1, int arg2) throws SQLException {
        ps.setObject(arg0, arg1, arg2);
    }

    public void setObject(int arg0, Object arg1) throws SQLException {
        ps.setObject(arg0, arg1);
    }

    public void setPoolable(boolean arg0) throws SQLException {
        ps.setPoolable(arg0);
    }

    public void setQueryTimeout(int arg0) throws SQLException {
        ps.setQueryTimeout(arg0);
    }

    public void setRef(int arg0, Ref arg1) throws SQLException {
        ps.setRef(arg0, arg1);
    }

    public void setRowId(int arg0, RowId arg1) throws SQLException {
        ps.setRowId(arg0, arg1);
    }

    public void setSQLXML(int arg0, SQLXML arg1) throws SQLException {
        ps.setSQLXML(arg0, arg1);
    }

    public void setShort(int arg0, short arg1) throws SQLException {
        ps.setShort(arg0, arg1);
    }

    public void setString(int arg0, String arg1) throws SQLException {
        ps.setString(arg0, arg1);
    }

    public void setTime(int arg0, Time arg1, Calendar arg2) throws SQLException {
        ps.setTime(arg0, arg1, arg2);
    }

    public void setTime(int arg0, Time arg1) throws SQLException {
        ps.setTime(arg0, arg1);
    }

    public void setTimestamp(int arg0, Timestamp arg1, Calendar arg2) throws SQLException {
        ps.setTimestamp(arg0, arg1, arg2);
    }

    public void setTimestamp(int arg0, Timestamp arg1) throws SQLException {
        ps.setTimestamp(arg0, arg1);
    }

    public void setURL(int arg0, URL arg1) throws SQLException {
        ps.setURL(arg0, arg1);
    }

    @Deprecated
    public void setUnicodeStream(int arg0, InputStream arg1, int arg2) throws SQLException {
        ps.setUnicodeStream(arg0, arg1, arg2);
    }

    public <T> T unwrap(Class<T> arg0) throws SQLException {
        return ps.unwrap(arg0);
    }

}
