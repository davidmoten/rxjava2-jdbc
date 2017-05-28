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
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.Map;

public final class TransactedResultSet implements ResultSet {

    private final ResultSet rs;
    private final TransactedPreparedStatement ps;

    TransactedResultSet(TransactedPreparedStatement ps, ResultSet rs) {
        this.rs = rs;
        this.ps = ps;
    }

    @Override
    public boolean absolute(int arg0) throws SQLException {
        return rs.absolute(arg0);
    }

    @Override
    public void afterLast() throws SQLException {
        rs.afterLast();
    }

    @Override
    public void beforeFirst() throws SQLException {
        rs.beforeFirst();
    }

    @Override
    public void cancelRowUpdates() throws SQLException {
        rs.cancelRowUpdates();
    }

    @Override
    public void clearWarnings() throws SQLException {
        rs.clearWarnings();
    }

    @Override
    public void close() throws SQLException {
        rs.close();
    }

    @Override
    public void deleteRow() throws SQLException {
        rs.deleteRow();
    }

    @Override
    public int findColumn(String arg0) throws SQLException {
        return rs.findColumn(arg0);
    }

    @Override
    public boolean first() throws SQLException {
        return rs.first();
    }

    @Override
    public Array getArray(int arg0) throws SQLException {
        return rs.getArray(arg0);
    }

    @Override
    public Array getArray(String arg0) throws SQLException {
        return rs.getArray(arg0);
    }

    @Override
    public InputStream getAsciiStream(int arg0) throws SQLException {
        return rs.getAsciiStream(arg0);
    }

    @Override
    public InputStream getAsciiStream(String arg0) throws SQLException {
        return rs.getAsciiStream(arg0);
    }

    @Override
    public BigDecimal getBigDecimal(int arg0) throws SQLException {
        return rs.getBigDecimal(arg0);
    }

    @Override
    public BigDecimal getBigDecimal(String arg0) throws SQLException {
        return rs.getBigDecimal(arg0);
    }

    @Override
    @Deprecated
    public BigDecimal getBigDecimal(int arg0, int arg1) throws SQLException {
        return rs.getBigDecimal(arg0, arg1);
    }

    @Override
    @Deprecated
    public BigDecimal getBigDecimal(String arg0, int arg1) throws SQLException {
        return rs.getBigDecimal(arg0, arg1);
    }

    @Override
    public InputStream getBinaryStream(int arg0) throws SQLException {
        return rs.getBinaryStream(arg0);
    }

    @Override
    public InputStream getBinaryStream(String arg0) throws SQLException {
        return rs.getBinaryStream(arg0);
    }

    @Override
    public Blob getBlob(int arg0) throws SQLException {
        return rs.getBlob(arg0);
    }

    @Override
    public Blob getBlob(String arg0) throws SQLException {
        return rs.getBlob(arg0);
    }

    @Override
    public boolean getBoolean(int arg0) throws SQLException {
        return rs.getBoolean(arg0);
    }

    @Override
    public boolean getBoolean(String arg0) throws SQLException {
        return rs.getBoolean(arg0);
    }

    @Override
    public byte getByte(int arg0) throws SQLException {
        return rs.getByte(arg0);
    }

    @Override
    public byte getByte(String arg0) throws SQLException {
        return rs.getByte(arg0);
    }

    @Override
    public byte[] getBytes(int arg0) throws SQLException {
        return rs.getBytes(arg0);
    }

    @Override
    public byte[] getBytes(String arg0) throws SQLException {
        return rs.getBytes(arg0);
    }

    @Override
    public Reader getCharacterStream(int arg0) throws SQLException {
        return rs.getCharacterStream(arg0);
    }

    @Override
    public Reader getCharacterStream(String arg0) throws SQLException {
        return rs.getCharacterStream(arg0);
    }

    @Override
    public Clob getClob(int arg0) throws SQLException {
        return rs.getClob(arg0);
    }

    @Override
    public Clob getClob(String arg0) throws SQLException {
        return rs.getClob(arg0);
    }

    @Override
    public int getConcurrency() throws SQLException {
        return rs.getConcurrency();
    }

    @Override
    public String getCursorName() throws SQLException {
        return rs.getCursorName();
    }

    @Override
    public Date getDate(int arg0) throws SQLException {
        return rs.getDate(arg0);
    }

    @Override
    public Date getDate(String arg0) throws SQLException {
        return rs.getDate(arg0);
    }

    @Override
    public Date getDate(int arg0, Calendar arg1) throws SQLException {
        return rs.getDate(arg0, arg1);
    }

    @Override
    public Date getDate(String arg0, Calendar arg1) throws SQLException {
        return rs.getDate(arg0, arg1);
    }

    @Override
    public double getDouble(int arg0) throws SQLException {
        return rs.getDouble(arg0);
    }

    @Override
    public double getDouble(String arg0) throws SQLException {
        return rs.getDouble(arg0);
    }

    @Override
    public int getFetchDirection() throws SQLException {
        return rs.getFetchDirection();
    }

    @Override
    public int getFetchSize() throws SQLException {
        return rs.getFetchSize();
    }

    @Override
    public float getFloat(int arg0) throws SQLException {
        return rs.getFloat(arg0);
    }

    @Override
    public float getFloat(String arg0) throws SQLException {
        return rs.getFloat(arg0);
    }

    @Override
    public int getHoldability() throws SQLException {
        return rs.getHoldability();
    }

    @Override
    public int getInt(int arg0) throws SQLException {
        return rs.getInt(arg0);
    }

    @Override
    public int getInt(String arg0) throws SQLException {
        return rs.getInt(arg0);
    }

    @Override
    public long getLong(int arg0) throws SQLException {
        return rs.getLong(arg0);
    }

    @Override
    public long getLong(String arg0) throws SQLException {
        return rs.getLong(arg0);
    }

    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        return rs.getMetaData();
    }

    @Override
    public Reader getNCharacterStream(int arg0) throws SQLException {
        return rs.getNCharacterStream(arg0);
    }

    @Override
    public Reader getNCharacterStream(String arg0) throws SQLException {
        return rs.getNCharacterStream(arg0);
    }

    @Override
    public NClob getNClob(int arg0) throws SQLException {
        return rs.getNClob(arg0);
    }

    @Override
    public NClob getNClob(String arg0) throws SQLException {
        return rs.getNClob(arg0);
    }

    @Override
    public String getNString(int arg0) throws SQLException {
        return rs.getNString(arg0);
    }

    @Override
    public String getNString(String arg0) throws SQLException {
        return rs.getNString(arg0);
    }

    @Override
    public Object getObject(int arg0) throws SQLException {
        return rs.getObject(arg0);
    }

    @Override
    public Object getObject(String arg0) throws SQLException {
        return rs.getObject(arg0);
    }

    @Override
    public Object getObject(int arg0, Map<String, Class<?>> arg1) throws SQLException {
        return rs.getObject(arg0, arg1);
    }

    @Override
    public Object getObject(String arg0, Map<String, Class<?>> arg1) throws SQLException {
        return rs.getObject(arg0, arg1);
    }

    @Override
    public <T> T getObject(int arg0, Class<T> arg1) throws SQLException {
        return rs.getObject(arg0, arg1);
    }

    @Override
    public <T> T getObject(String arg0, Class<T> arg1) throws SQLException {
        return rs.getObject(arg0, arg1);
    }

    @Override
    public Ref getRef(int arg0) throws SQLException {
        return rs.getRef(arg0);
    }

    @Override
    public Ref getRef(String arg0) throws SQLException {
        return rs.getRef(arg0);
    }

    @Override
    public int getRow() throws SQLException {
        return rs.getRow();
    }

    @Override
    public RowId getRowId(int arg0) throws SQLException {
        return rs.getRowId(arg0);
    }

    @Override
    public RowId getRowId(String arg0) throws SQLException {
        return rs.getRowId(arg0);
    }

    @Override
    public SQLXML getSQLXML(int arg0) throws SQLException {
        return rs.getSQLXML(arg0);
    }

    @Override
    public SQLXML getSQLXML(String arg0) throws SQLException {
        return rs.getSQLXML(arg0);
    }

    @Override
    public short getShort(int arg0) throws SQLException {
        return rs.getShort(arg0);
    }

    @Override
    public short getShort(String arg0) throws SQLException {
        return rs.getShort(arg0);
    }

    @Override
    public TransactedPreparedStatement getStatement() throws SQLException {
        return ps;
    }

    @Override
    public String getString(int arg0) throws SQLException {
        return rs.getString(arg0);
    }

    @Override
    public String getString(String arg0) throws SQLException {
        return rs.getString(arg0);
    }

    @Override
    public Time getTime(int arg0) throws SQLException {
        return rs.getTime(arg0);
    }

    @Override
    public Time getTime(String arg0) throws SQLException {
        return rs.getTime(arg0);
    }

    @Override
    public Time getTime(int arg0, Calendar arg1) throws SQLException {
        return rs.getTime(arg0, arg1);
    }

    @Override
    public Time getTime(String arg0, Calendar arg1) throws SQLException {
        return rs.getTime(arg0, arg1);
    }

    @Override
    public Timestamp getTimestamp(int arg0) throws SQLException {
        return rs.getTimestamp(arg0);
    }

    @Override
    public Timestamp getTimestamp(String arg0) throws SQLException {
        return rs.getTimestamp(arg0);
    }

    @Override
    public Timestamp getTimestamp(int arg0, Calendar arg1) throws SQLException {
        return rs.getTimestamp(arg0, arg1);
    }

    @Override
    public Timestamp getTimestamp(String arg0, Calendar arg1) throws SQLException {
        return rs.getTimestamp(arg0, arg1);
    }

    @Override
    public int getType() throws SQLException {
        return rs.getType();
    }

    @Override
    public URL getURL(int arg0) throws SQLException {
        return rs.getURL(arg0);
    }

    @Override
    public URL getURL(String arg0) throws SQLException {
        return rs.getURL(arg0);
    }

    @Override
    @Deprecated
    public InputStream getUnicodeStream(int arg0) throws SQLException {
        return rs.getUnicodeStream(arg0);
    }

    @Override
    @Deprecated
    public InputStream getUnicodeStream(String arg0) throws SQLException {
        return rs.getUnicodeStream(arg0);
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        return rs.getWarnings();
    }

    @Override
    public void insertRow() throws SQLException {
        rs.insertRow();
    }

    @Override
    public boolean isAfterLast() throws SQLException {
        return rs.isAfterLast();
    }

    @Override
    public boolean isBeforeFirst() throws SQLException {
        return rs.isBeforeFirst();
    }

    @Override
    public boolean isClosed() throws SQLException {
        return rs.isClosed();
    }

    @Override
    public boolean isFirst() throws SQLException {
        return rs.isFirst();
    }

    @Override
    public boolean isLast() throws SQLException {
        return rs.isLast();
    }

    @Override
    public boolean isWrapperFor(Class<?> arg0) throws SQLException {
        return rs.isWrapperFor(arg0);
    }

    @Override
    public boolean last() throws SQLException {
        return rs.last();
    }

    @Override
    public void moveToCurrentRow() throws SQLException {
        rs.moveToCurrentRow();
    }

    @Override
    public void moveToInsertRow() throws SQLException {
        rs.moveToInsertRow();
    }

    @Override
    public boolean next() throws SQLException {
        return rs.next();
    }

    @Override
    public boolean previous() throws SQLException {
        return rs.previous();
    }

    @Override
    public void refreshRow() throws SQLException {
        rs.refreshRow();
    }

    @Override
    public boolean relative(int arg0) throws SQLException {
        return rs.relative(arg0);
    }

    @Override
    public boolean rowDeleted() throws SQLException {
        return rs.rowDeleted();
    }

    @Override
    public boolean rowInserted() throws SQLException {
        return rs.rowInserted();
    }

    @Override
    public boolean rowUpdated() throws SQLException {
        return rs.rowUpdated();
    }

    @Override
    public void setFetchDirection(int arg0) throws SQLException {
        rs.setFetchDirection(arg0);
    }

    @Override
    public void setFetchSize(int arg0) throws SQLException {
        rs.setFetchSize(arg0);
    }

    @Override
    public <T> T unwrap(Class<T> arg0) throws SQLException {
        return rs.unwrap(arg0);
    }

    @Override
    public void updateArray(int arg0, Array arg1) throws SQLException {
        rs.updateArray(arg0, arg1);
    }

    @Override
    public void updateArray(String arg0, Array arg1) throws SQLException {
        rs.updateArray(arg0, arg1);
    }

    @Override
    public void updateAsciiStream(int arg0, InputStream arg1) throws SQLException {
        rs.updateAsciiStream(arg0, arg1);
    }

    @Override
    public void updateAsciiStream(String arg0, InputStream arg1) throws SQLException {
        rs.updateAsciiStream(arg0, arg1);
    }

    @Override
    public void updateAsciiStream(int arg0, InputStream arg1, int arg2) throws SQLException {
        rs.updateAsciiStream(arg0, arg1, arg2);
    }

    @Override
    public void updateAsciiStream(String arg0, InputStream arg1, int arg2) throws SQLException {
        rs.updateAsciiStream(arg0, arg1, arg2);
    }

    @Override
    public void updateAsciiStream(int arg0, InputStream arg1, long arg2) throws SQLException {
        rs.updateAsciiStream(arg0, arg1, arg2);
    }

    @Override
    public void updateAsciiStream(String arg0, InputStream arg1, long arg2) throws SQLException {
        rs.updateAsciiStream(arg0, arg1, arg2);
    }

    @Override
    public void updateBigDecimal(int arg0, BigDecimal arg1) throws SQLException {
        rs.updateBigDecimal(arg0, arg1);
    }

    @Override
    public void updateBigDecimal(String arg0, BigDecimal arg1) throws SQLException {
        rs.updateBigDecimal(arg0, arg1);
    }

    @Override
    public void updateBinaryStream(int arg0, InputStream arg1) throws SQLException {
        rs.updateBinaryStream(arg0, arg1);
    }

    @Override
    public void updateBinaryStream(String arg0, InputStream arg1) throws SQLException {
        rs.updateBinaryStream(arg0, arg1);
    }

    @Override
    public void updateBinaryStream(int arg0, InputStream arg1, int arg2) throws SQLException {
        rs.updateBinaryStream(arg0, arg1, arg2);
    }

    @Override
    public void updateBinaryStream(String arg0, InputStream arg1, int arg2) throws SQLException {
        rs.updateBinaryStream(arg0, arg1, arg2);
    }

    @Override
    public void updateBinaryStream(int arg0, InputStream arg1, long arg2) throws SQLException {
        rs.updateBinaryStream(arg0, arg1, arg2);
    }

    @Override
    public void updateBinaryStream(String arg0, InputStream arg1, long arg2) throws SQLException {
        rs.updateBinaryStream(arg0, arg1, arg2);
    }

    @Override
    public void updateBlob(int arg0, Blob arg1) throws SQLException {
        rs.updateBlob(arg0, arg1);
    }

    @Override
    public void updateBlob(String arg0, Blob arg1) throws SQLException {
        rs.updateBlob(arg0, arg1);
    }

    @Override
    public void updateBlob(int arg0, InputStream arg1) throws SQLException {
        rs.updateBlob(arg0, arg1);
    }

    @Override
    public void updateBlob(String arg0, InputStream arg1) throws SQLException {
        rs.updateBlob(arg0, arg1);
    }

    @Override
    public void updateBlob(int arg0, InputStream arg1, long arg2) throws SQLException {
        rs.updateBlob(arg0, arg1, arg2);
    }

    @Override
    public void updateBlob(String arg0, InputStream arg1, long arg2) throws SQLException {
        rs.updateBlob(arg0, arg1, arg2);
    }

    @Override
    public void updateBoolean(int arg0, boolean arg1) throws SQLException {
        rs.updateBoolean(arg0, arg1);
    }

    @Override
    public void updateBoolean(String arg0, boolean arg1) throws SQLException {
        rs.updateBoolean(arg0, arg1);
    }

    @Override
    public void updateByte(int arg0, byte arg1) throws SQLException {
        rs.updateByte(arg0, arg1);
    }

    @Override
    public void updateByte(String arg0, byte arg1) throws SQLException {
        rs.updateByte(arg0, arg1);
    }

    @Override
    public void updateBytes(int arg0, byte[] arg1) throws SQLException {
        rs.updateBytes(arg0, arg1);
    }

    @Override
    public void updateBytes(String arg0, byte[] arg1) throws SQLException {
        rs.updateBytes(arg0, arg1);
    }

    @Override
    public void updateCharacterStream(int arg0, Reader arg1) throws SQLException {
        rs.updateCharacterStream(arg0, arg1);
    }

    @Override
    public void updateCharacterStream(String arg0, Reader arg1) throws SQLException {
        rs.updateCharacterStream(arg0, arg1);
    }

    @Override
    public void updateCharacterStream(int arg0, Reader arg1, int arg2) throws SQLException {
        rs.updateCharacterStream(arg0, arg1, arg2);
    }

    @Override
    public void updateCharacterStream(String arg0, Reader arg1, int arg2) throws SQLException {
        rs.updateCharacterStream(arg0, arg1, arg2);
    }

    @Override
    public void updateCharacterStream(int arg0, Reader arg1, long arg2) throws SQLException {
        rs.updateCharacterStream(arg0, arg1, arg2);
    }

    @Override
    public void updateCharacterStream(String arg0, Reader arg1, long arg2) throws SQLException {
        rs.updateCharacterStream(arg0, arg1, arg2);
    }

    @Override
    public void updateClob(int arg0, Clob arg1) throws SQLException {
        rs.updateClob(arg0, arg1);
    }

    @Override
    public void updateClob(String arg0, Clob arg1) throws SQLException {
        rs.updateClob(arg0, arg1);
    }

    @Override
    public void updateClob(int arg0, Reader arg1) throws SQLException {
        rs.updateClob(arg0, arg1);
    }

    @Override
    public void updateClob(String arg0, Reader arg1) throws SQLException {
        rs.updateClob(arg0, arg1);
    }

    @Override
    public void updateClob(int arg0, Reader arg1, long arg2) throws SQLException {
        rs.updateClob(arg0, arg1, arg2);
    }

    @Override
    public void updateClob(String arg0, Reader arg1, long arg2) throws SQLException {
        rs.updateClob(arg0, arg1, arg2);
    }

    @Override
    public void updateDate(int arg0, Date arg1) throws SQLException {
        rs.updateDate(arg0, arg1);
    }

    @Override
    public void updateDate(String arg0, Date arg1) throws SQLException {
        rs.updateDate(arg0, arg1);
    }

    @Override
    public void updateDouble(int arg0, double arg1) throws SQLException {
        rs.updateDouble(arg0, arg1);
    }

    @Override
    public void updateDouble(String arg0, double arg1) throws SQLException {
        rs.updateDouble(arg0, arg1);
    }

    @Override
    public void updateFloat(int arg0, float arg1) throws SQLException {
        rs.updateFloat(arg0, arg1);
    }

    @Override
    public void updateFloat(String arg0, float arg1) throws SQLException {
        rs.updateFloat(arg0, arg1);
    }

    @Override
    public void updateInt(int arg0, int arg1) throws SQLException {
        rs.updateInt(arg0, arg1);
    }

    @Override
    public void updateInt(String arg0, int arg1) throws SQLException {
        rs.updateInt(arg0, arg1);
    }

    @Override
    public void updateLong(int arg0, long arg1) throws SQLException {
        rs.updateLong(arg0, arg1);
    }

    @Override
    public void updateLong(String arg0, long arg1) throws SQLException {
        rs.updateLong(arg0, arg1);
    }

    @Override
    public void updateNCharacterStream(int arg0, Reader arg1) throws SQLException {
        rs.updateNCharacterStream(arg0, arg1);
    }

    @Override
    public void updateNCharacterStream(String arg0, Reader arg1) throws SQLException {
        rs.updateNCharacterStream(arg0, arg1);
    }

    @Override
    public void updateNCharacterStream(int arg0, Reader arg1, long arg2) throws SQLException {
        rs.updateNCharacterStream(arg0, arg1, arg2);
    }

    @Override
    public void updateNCharacterStream(String arg0, Reader arg1, long arg2) throws SQLException {
        rs.updateNCharacterStream(arg0, arg1, arg2);
    }

    @Override
    public void updateNClob(int arg0, NClob arg1) throws SQLException {
        rs.updateNClob(arg0, arg1);
    }

    @Override
    public void updateNClob(String arg0, NClob arg1) throws SQLException {
        rs.updateNClob(arg0, arg1);
    }

    @Override
    public void updateNClob(int arg0, Reader arg1) throws SQLException {
        rs.updateNClob(arg0, arg1);
    }

    @Override
    public void updateNClob(String arg0, Reader arg1) throws SQLException {
        rs.updateNClob(arg0, arg1);
    }

    @Override
    public void updateNClob(int arg0, Reader arg1, long arg2) throws SQLException {
        rs.updateNClob(arg0, arg1, arg2);
    }

    @Override
    public void updateNClob(String arg0, Reader arg1, long arg2) throws SQLException {
        rs.updateNClob(arg0, arg1, arg2);
    }

    @Override
    public void updateNString(int arg0, String arg1) throws SQLException {
        rs.updateNString(arg0, arg1);
    }

    @Override
    public void updateNString(String arg0, String arg1) throws SQLException {
        rs.updateNString(arg0, arg1);
    }

    @Override
    public void updateNull(int arg0) throws SQLException {
        rs.updateNull(arg0);
    }

    @Override
    public void updateNull(String arg0) throws SQLException {
        rs.updateNull(arg0);
    }

    @Override
    public void updateObject(int arg0, Object arg1) throws SQLException {
        rs.updateObject(arg0, arg1);
    }

    @Override
    public void updateObject(String arg0, Object arg1) throws SQLException {
        rs.updateObject(arg0, arg1);
    }

    @Override
    public void updateObject(int arg0, Object arg1, int arg2) throws SQLException {
        rs.updateObject(arg0, arg1, arg2);
    }

    @Override
    public void updateObject(String arg0, Object arg1, int arg2) throws SQLException {
        rs.updateObject(arg0, arg1, arg2);
    }

    @Override
    public void updateRef(int arg0, Ref arg1) throws SQLException {
        rs.updateRef(arg0, arg1);
    }

    @Override
    public void updateRef(String arg0, Ref arg1) throws SQLException {
        rs.updateRef(arg0, arg1);
    }

    @Override
    public void updateRow() throws SQLException {
        rs.updateRow();
    }

    @Override
    public void updateRowId(int arg0, RowId arg1) throws SQLException {
        rs.updateRowId(arg0, arg1);
    }

    @Override
    public void updateRowId(String arg0, RowId arg1) throws SQLException {
        rs.updateRowId(arg0, arg1);
    }

    @Override
    public void updateSQLXML(int arg0, SQLXML arg1) throws SQLException {
        rs.updateSQLXML(arg0, arg1);
    }

    @Override
    public void updateSQLXML(String arg0, SQLXML arg1) throws SQLException {
        rs.updateSQLXML(arg0, arg1);
    }

    @Override
    public void updateShort(int arg0, short arg1) throws SQLException {
        rs.updateShort(arg0, arg1);
    }

    @Override
    public void updateShort(String arg0, short arg1) throws SQLException {
        rs.updateShort(arg0, arg1);
    }

    @Override
    public void updateString(int arg0, String arg1) throws SQLException {
        rs.updateString(arg0, arg1);
    }

    @Override
    public void updateString(String arg0, String arg1) throws SQLException {
        rs.updateString(arg0, arg1);
    }

    @Override
    public void updateTime(int arg0, Time arg1) throws SQLException {
        rs.updateTime(arg0, arg1);
    }

    @Override
    public void updateTime(String arg0, Time arg1) throws SQLException {
        rs.updateTime(arg0, arg1);
    }

    @Override
    public void updateTimestamp(int arg0, Timestamp arg1) throws SQLException {
        rs.updateTimestamp(arg0, arg1);
    }

    @Override
    public void updateTimestamp(String arg0, Timestamp arg1) throws SQLException {
        rs.updateTimestamp(arg0, arg1);
    }

    @Override
    public boolean wasNull() throws SQLException {
        return rs.wasNull();
    }

}
