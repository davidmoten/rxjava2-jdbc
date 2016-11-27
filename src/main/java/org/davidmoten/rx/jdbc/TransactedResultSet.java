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

public class TransactedResultSet implements ResultSet{

    private final ResultSet rs;
    private final TransactedPreparedStatement ps;

    public TransactedResultSet(TransactedPreparedStatement ps, ResultSet rs) {
        this.rs = rs;
        this.ps = ps;
    }

    public boolean absolute(int arg0) throws SQLException {
        return rs.absolute(arg0);
    }

    public void afterLast() throws SQLException {
        rs.afterLast();
    }

    public void beforeFirst() throws SQLException {
        rs.beforeFirst();
    }

    public void cancelRowUpdates() throws SQLException {
        rs.cancelRowUpdates();
    }

    public void clearWarnings() throws SQLException {
        rs.clearWarnings();
    }

    public void close() throws SQLException {
        rs.close();
    }

    public void deleteRow() throws SQLException {
        rs.deleteRow();
    }

    public int findColumn(String arg0) throws SQLException {
        return rs.findColumn(arg0);
    }

    public boolean first() throws SQLException {
        return rs.first();
    }

    public Array getArray(int arg0) throws SQLException {
        return rs.getArray(arg0);
    }

    public Array getArray(String arg0) throws SQLException {
        return rs.getArray(arg0);
    }

    public InputStream getAsciiStream(int arg0) throws SQLException {
        return rs.getAsciiStream(arg0);
    }

    public InputStream getAsciiStream(String arg0) throws SQLException {
        return rs.getAsciiStream(arg0);
    }

    public BigDecimal getBigDecimal(int arg0) throws SQLException {
        return rs.getBigDecimal(arg0);
    }

    public BigDecimal getBigDecimal(String arg0) throws SQLException {
        return rs.getBigDecimal(arg0);
    }

    public BigDecimal getBigDecimal(int arg0, int arg1) throws SQLException {
        return rs.getBigDecimal(arg0, arg1);
    }

    public BigDecimal getBigDecimal(String arg0, int arg1) throws SQLException {
        return rs.getBigDecimal(arg0, arg1);
    }

    public InputStream getBinaryStream(int arg0) throws SQLException {
        return rs.getBinaryStream(arg0);
    }

    public InputStream getBinaryStream(String arg0) throws SQLException {
        return rs.getBinaryStream(arg0);
    }

    public Blob getBlob(int arg0) throws SQLException {
        return rs.getBlob(arg0);
    }

    public Blob getBlob(String arg0) throws SQLException {
        return rs.getBlob(arg0);
    }

    public boolean getBoolean(int arg0) throws SQLException {
        return rs.getBoolean(arg0);
    }

    public boolean getBoolean(String arg0) throws SQLException {
        return rs.getBoolean(arg0);
    }

    public byte getByte(int arg0) throws SQLException {
        return rs.getByte(arg0);
    }

    public byte getByte(String arg0) throws SQLException {
        return rs.getByte(arg0);
    }

    public byte[] getBytes(int arg0) throws SQLException {
        return rs.getBytes(arg0);
    }

    public byte[] getBytes(String arg0) throws SQLException {
        return rs.getBytes(arg0);
    }

    public Reader getCharacterStream(int arg0) throws SQLException {
        return rs.getCharacterStream(arg0);
    }

    public Reader getCharacterStream(String arg0) throws SQLException {
        return rs.getCharacterStream(arg0);
    }

    public Clob getClob(int arg0) throws SQLException {
        return rs.getClob(arg0);
    }

    public Clob getClob(String arg0) throws SQLException {
        return rs.getClob(arg0);
    }

    public int getConcurrency() throws SQLException {
        return rs.getConcurrency();
    }

    public String getCursorName() throws SQLException {
        return rs.getCursorName();
    }

    public Date getDate(int arg0) throws SQLException {
        return rs.getDate(arg0);
    }

    public Date getDate(String arg0) throws SQLException {
        return rs.getDate(arg0);
    }

    public Date getDate(int arg0, Calendar arg1) throws SQLException {
        return rs.getDate(arg0, arg1);
    }

    public Date getDate(String arg0, Calendar arg1) throws SQLException {
        return rs.getDate(arg0, arg1);
    }

    public double getDouble(int arg0) throws SQLException {
        return rs.getDouble(arg0);
    }

    public double getDouble(String arg0) throws SQLException {
        return rs.getDouble(arg0);
    }

    public int getFetchDirection() throws SQLException {
        return rs.getFetchDirection();
    }

    public int getFetchSize() throws SQLException {
        return rs.getFetchSize();
    }

    public float getFloat(int arg0) throws SQLException {
        return rs.getFloat(arg0);
    }

    public float getFloat(String arg0) throws SQLException {
        return rs.getFloat(arg0);
    }

    public int getHoldability() throws SQLException {
        return rs.getHoldability();
    }

    public int getInt(int arg0) throws SQLException {
        return rs.getInt(arg0);
    }

    public int getInt(String arg0) throws SQLException {
        return rs.getInt(arg0);
    }

    public long getLong(int arg0) throws SQLException {
        return rs.getLong(arg0);
    }

    public long getLong(String arg0) throws SQLException {
        return rs.getLong(arg0);
    }

    public ResultSetMetaData getMetaData() throws SQLException {
        return rs.getMetaData();
    }

    public Reader getNCharacterStream(int arg0) throws SQLException {
        return rs.getNCharacterStream(arg0);
    }

    public Reader getNCharacterStream(String arg0) throws SQLException {
        return rs.getNCharacterStream(arg0);
    }

    public NClob getNClob(int arg0) throws SQLException {
        return rs.getNClob(arg0);
    }

    public NClob getNClob(String arg0) throws SQLException {
        return rs.getNClob(arg0);
    }

    public String getNString(int arg0) throws SQLException {
        return rs.getNString(arg0);
    }

    public String getNString(String arg0) throws SQLException {
        return rs.getNString(arg0);
    }

    public Object getObject(int arg0) throws SQLException {
        return rs.getObject(arg0);
    }

    public Object getObject(String arg0) throws SQLException {
        return rs.getObject(arg0);
    }

    public Object getObject(int arg0, Map<String, Class<?>> arg1) throws SQLException {
        return rs.getObject(arg0, arg1);
    }

    public Object getObject(String arg0, Map<String, Class<?>> arg1) throws SQLException {
        return rs.getObject(arg0, arg1);
    }

    public <T> T getObject(int arg0, Class<T> arg1) throws SQLException {
        return rs.getObject(arg0, arg1);
    }

    public <T> T getObject(String arg0, Class<T> arg1) throws SQLException {
        return rs.getObject(arg0, arg1);
    }

    public Ref getRef(int arg0) throws SQLException {
        return rs.getRef(arg0);
    }

    public Ref getRef(String arg0) throws SQLException {
        return rs.getRef(arg0);
    }

    public int getRow() throws SQLException {
        return rs.getRow();
    }

    public RowId getRowId(int arg0) throws SQLException {
        return rs.getRowId(arg0);
    }

    public RowId getRowId(String arg0) throws SQLException {
        return rs.getRowId(arg0);
    }

    public SQLXML getSQLXML(int arg0) throws SQLException {
        return rs.getSQLXML(arg0);
    }

    public SQLXML getSQLXML(String arg0) throws SQLException {
        return rs.getSQLXML(arg0);
    }

    public short getShort(int arg0) throws SQLException {
        return rs.getShort(arg0);
    }

    public short getShort(String arg0) throws SQLException {
        return rs.getShort(arg0);
    }

    public TransactedPreparedStatement getStatement() throws SQLException {
        return ps;
    }

    public String getString(int arg0) throws SQLException {
        return rs.getString(arg0);
    }

    public String getString(String arg0) throws SQLException {
        return rs.getString(arg0);
    }

    public Time getTime(int arg0) throws SQLException {
        return rs.getTime(arg0);
    }

    public Time getTime(String arg0) throws SQLException {
        return rs.getTime(arg0);
    }

    public Time getTime(int arg0, Calendar arg1) throws SQLException {
        return rs.getTime(arg0, arg1);
    }

    public Time getTime(String arg0, Calendar arg1) throws SQLException {
        return rs.getTime(arg0, arg1);
    }

    public Timestamp getTimestamp(int arg0) throws SQLException {
        return rs.getTimestamp(arg0);
    }

    public Timestamp getTimestamp(String arg0) throws SQLException {
        return rs.getTimestamp(arg0);
    }

    public Timestamp getTimestamp(int arg0, Calendar arg1) throws SQLException {
        return rs.getTimestamp(arg0, arg1);
    }

    public Timestamp getTimestamp(String arg0, Calendar arg1) throws SQLException {
        return rs.getTimestamp(arg0, arg1);
    }

    public int getType() throws SQLException {
        return rs.getType();
    }

    public URL getURL(int arg0) throws SQLException {
        return rs.getURL(arg0);
    }

    public URL getURL(String arg0) throws SQLException {
        return rs.getURL(arg0);
    }

    @Deprecated
    public InputStream getUnicodeStream(int arg0) throws SQLException {
        return rs.getUnicodeStream(arg0);
    }

    @Deprecated
    public InputStream getUnicodeStream(String arg0) throws SQLException {
        return rs.getUnicodeStream(arg0);
    }

    public SQLWarning getWarnings() throws SQLException {
        return rs.getWarnings();
    }

    public void insertRow() throws SQLException {
        rs.insertRow();
    }

    public boolean isAfterLast() throws SQLException {
        return rs.isAfterLast();
    }

    public boolean isBeforeFirst() throws SQLException {
        return rs.isBeforeFirst();
    }

    public boolean isClosed() throws SQLException {
        return rs.isClosed();
    }

    public boolean isFirst() throws SQLException {
        return rs.isFirst();
    }

    public boolean isLast() throws SQLException {
        return rs.isLast();
    }

    public boolean isWrapperFor(Class<?> arg0) throws SQLException {
        return rs.isWrapperFor(arg0);
    }

    public boolean last() throws SQLException {
        return rs.last();
    }

    public void moveToCurrentRow() throws SQLException {
        rs.moveToCurrentRow();
    }

    public void moveToInsertRow() throws SQLException {
        rs.moveToInsertRow();
    }

    public boolean next() throws SQLException {
        return rs.next();
    }

    public boolean previous() throws SQLException {
        return rs.previous();
    }

    public void refreshRow() throws SQLException {
        rs.refreshRow();
    }

    public boolean relative(int arg0) throws SQLException {
        return rs.relative(arg0);
    }

    public boolean rowDeleted() throws SQLException {
        return rs.rowDeleted();
    }

    public boolean rowInserted() throws SQLException {
        return rs.rowInserted();
    }

    public boolean rowUpdated() throws SQLException {
        return rs.rowUpdated();
    }

    public void setFetchDirection(int arg0) throws SQLException {
        rs.setFetchDirection(arg0);
    }

    public void setFetchSize(int arg0) throws SQLException {
        rs.setFetchSize(arg0);
    }

    public <T> T unwrap(Class<T> arg0) throws SQLException {
        return rs.unwrap(arg0);
    }

    public void updateArray(int arg0, Array arg1) throws SQLException {
        rs.updateArray(arg0, arg1);
    }

    public void updateArray(String arg0, Array arg1) throws SQLException {
        rs.updateArray(arg0, arg1);
    }

    public void updateAsciiStream(int arg0, InputStream arg1) throws SQLException {
        rs.updateAsciiStream(arg0, arg1);
    }

    public void updateAsciiStream(String arg0, InputStream arg1) throws SQLException {
        rs.updateAsciiStream(arg0, arg1);
    }

    public void updateAsciiStream(int arg0, InputStream arg1, int arg2) throws SQLException {
        rs.updateAsciiStream(arg0, arg1, arg2);
    }

    public void updateAsciiStream(String arg0, InputStream arg1, int arg2) throws SQLException {
        rs.updateAsciiStream(arg0, arg1, arg2);
    }

    public void updateAsciiStream(int arg0, InputStream arg1, long arg2) throws SQLException {
        rs.updateAsciiStream(arg0, arg1, arg2);
    }

    public void updateAsciiStream(String arg0, InputStream arg1, long arg2) throws SQLException {
        rs.updateAsciiStream(arg0, arg1, arg2);
    }

    public void updateBigDecimal(int arg0, BigDecimal arg1) throws SQLException {
        rs.updateBigDecimal(arg0, arg1);
    }

    public void updateBigDecimal(String arg0, BigDecimal arg1) throws SQLException {
        rs.updateBigDecimal(arg0, arg1);
    }

    public void updateBinaryStream(int arg0, InputStream arg1) throws SQLException {
        rs.updateBinaryStream(arg0, arg1);
    }

    public void updateBinaryStream(String arg0, InputStream arg1) throws SQLException {
        rs.updateBinaryStream(arg0, arg1);
    }

    public void updateBinaryStream(int arg0, InputStream arg1, int arg2) throws SQLException {
        rs.updateBinaryStream(arg0, arg1, arg2);
    }

    public void updateBinaryStream(String arg0, InputStream arg1, int arg2) throws SQLException {
        rs.updateBinaryStream(arg0, arg1, arg2);
    }

    public void updateBinaryStream(int arg0, InputStream arg1, long arg2) throws SQLException {
        rs.updateBinaryStream(arg0, arg1, arg2);
    }

    public void updateBinaryStream(String arg0, InputStream arg1, long arg2) throws SQLException {
        rs.updateBinaryStream(arg0, arg1, arg2);
    }

    public void updateBlob(int arg0, Blob arg1) throws SQLException {
        rs.updateBlob(arg0, arg1);
    }

    public void updateBlob(String arg0, Blob arg1) throws SQLException {
        rs.updateBlob(arg0, arg1);
    }

    public void updateBlob(int arg0, InputStream arg1) throws SQLException {
        rs.updateBlob(arg0, arg1);
    }

    public void updateBlob(String arg0, InputStream arg1) throws SQLException {
        rs.updateBlob(arg0, arg1);
    }

    public void updateBlob(int arg0, InputStream arg1, long arg2) throws SQLException {
        rs.updateBlob(arg0, arg1, arg2);
    }

    public void updateBlob(String arg0, InputStream arg1, long arg2) throws SQLException {
        rs.updateBlob(arg0, arg1, arg2);
    }

    public void updateBoolean(int arg0, boolean arg1) throws SQLException {
        rs.updateBoolean(arg0, arg1);
    }

    public void updateBoolean(String arg0, boolean arg1) throws SQLException {
        rs.updateBoolean(arg0, arg1);
    }

    public void updateByte(int arg0, byte arg1) throws SQLException {
        rs.updateByte(arg0, arg1);
    }

    public void updateByte(String arg0, byte arg1) throws SQLException {
        rs.updateByte(arg0, arg1);
    }

    public void updateBytes(int arg0, byte[] arg1) throws SQLException {
        rs.updateBytes(arg0, arg1);
    }

    public void updateBytes(String arg0, byte[] arg1) throws SQLException {
        rs.updateBytes(arg0, arg1);
    }

    public void updateCharacterStream(int arg0, Reader arg1) throws SQLException {
        rs.updateCharacterStream(arg0, arg1);
    }

    public void updateCharacterStream(String arg0, Reader arg1) throws SQLException {
        rs.updateCharacterStream(arg0, arg1);
    }

    public void updateCharacterStream(int arg0, Reader arg1, int arg2) throws SQLException {
        rs.updateCharacterStream(arg0, arg1, arg2);
    }

    public void updateCharacterStream(String arg0, Reader arg1, int arg2) throws SQLException {
        rs.updateCharacterStream(arg0, arg1, arg2);
    }

    public void updateCharacterStream(int arg0, Reader arg1, long arg2) throws SQLException {
        rs.updateCharacterStream(arg0, arg1, arg2);
    }

    public void updateCharacterStream(String arg0, Reader arg1, long arg2) throws SQLException {
        rs.updateCharacterStream(arg0, arg1, arg2);
    }

    public void updateClob(int arg0, Clob arg1) throws SQLException {
        rs.updateClob(arg0, arg1);
    }

    public void updateClob(String arg0, Clob arg1) throws SQLException {
        rs.updateClob(arg0, arg1);
    }

    public void updateClob(int arg0, Reader arg1) throws SQLException {
        rs.updateClob(arg0, arg1);
    }

    public void updateClob(String arg0, Reader arg1) throws SQLException {
        rs.updateClob(arg0, arg1);
    }

    public void updateClob(int arg0, Reader arg1, long arg2) throws SQLException {
        rs.updateClob(arg0, arg1, arg2);
    }

    public void updateClob(String arg0, Reader arg1, long arg2) throws SQLException {
        rs.updateClob(arg0, arg1, arg2);
    }

    public void updateDate(int arg0, Date arg1) throws SQLException {
        rs.updateDate(arg0, arg1);
    }

    public void updateDate(String arg0, Date arg1) throws SQLException {
        rs.updateDate(arg0, arg1);
    }

    public void updateDouble(int arg0, double arg1) throws SQLException {
        rs.updateDouble(arg0, arg1);
    }

    public void updateDouble(String arg0, double arg1) throws SQLException {
        rs.updateDouble(arg0, arg1);
    }

    public void updateFloat(int arg0, float arg1) throws SQLException {
        rs.updateFloat(arg0, arg1);
    }

    public void updateFloat(String arg0, float arg1) throws SQLException {
        rs.updateFloat(arg0, arg1);
    }

    public void updateInt(int arg0, int arg1) throws SQLException {
        rs.updateInt(arg0, arg1);
    }

    public void updateInt(String arg0, int arg1) throws SQLException {
        rs.updateInt(arg0, arg1);
    }

    public void updateLong(int arg0, long arg1) throws SQLException {
        rs.updateLong(arg0, arg1);
    }

    public void updateLong(String arg0, long arg1) throws SQLException {
        rs.updateLong(arg0, arg1);
    }

    public void updateNCharacterStream(int arg0, Reader arg1) throws SQLException {
        rs.updateNCharacterStream(arg0, arg1);
    }

    public void updateNCharacterStream(String arg0, Reader arg1) throws SQLException {
        rs.updateNCharacterStream(arg0, arg1);
    }

    public void updateNCharacterStream(int arg0, Reader arg1, long arg2) throws SQLException {
        rs.updateNCharacterStream(arg0, arg1, arg2);
    }

    public void updateNCharacterStream(String arg0, Reader arg1, long arg2) throws SQLException {
        rs.updateNCharacterStream(arg0, arg1, arg2);
    }

    public void updateNClob(int arg0, NClob arg1) throws SQLException {
        rs.updateNClob(arg0, arg1);
    }

    public void updateNClob(String arg0, NClob arg1) throws SQLException {
        rs.updateNClob(arg0, arg1);
    }

    public void updateNClob(int arg0, Reader arg1) throws SQLException {
        rs.updateNClob(arg0, arg1);
    }

    public void updateNClob(String arg0, Reader arg1) throws SQLException {
        rs.updateNClob(arg0, arg1);
    }

    public void updateNClob(int arg0, Reader arg1, long arg2) throws SQLException {
        rs.updateNClob(arg0, arg1, arg2);
    }

    public void updateNClob(String arg0, Reader arg1, long arg2) throws SQLException {
        rs.updateNClob(arg0, arg1, arg2);
    }

    public void updateNString(int arg0, String arg1) throws SQLException {
        rs.updateNString(arg0, arg1);
    }

    public void updateNString(String arg0, String arg1) throws SQLException {
        rs.updateNString(arg0, arg1);
    }

    public void updateNull(int arg0) throws SQLException {
        rs.updateNull(arg0);
    }

    public void updateNull(String arg0) throws SQLException {
        rs.updateNull(arg0);
    }

    public void updateObject(int arg0, Object arg1) throws SQLException {
        rs.updateObject(arg0, arg1);
    }

    public void updateObject(String arg0, Object arg1) throws SQLException {
        rs.updateObject(arg0, arg1);
    }

    public void updateObject(int arg0, Object arg1, int arg2) throws SQLException {
        rs.updateObject(arg0, arg1, arg2);
    }

    public void updateObject(String arg0, Object arg1, int arg2) throws SQLException {
        rs.updateObject(arg0, arg1, arg2);
    }

    public void updateRef(int arg0, Ref arg1) throws SQLException {
        rs.updateRef(arg0, arg1);
    }

    public void updateRef(String arg0, Ref arg1) throws SQLException {
        rs.updateRef(arg0, arg1);
    }

    public void updateRow() throws SQLException {
        rs.updateRow();
    }

    public void updateRowId(int arg0, RowId arg1) throws SQLException {
        rs.updateRowId(arg0, arg1);
    }

    public void updateRowId(String arg0, RowId arg1) throws SQLException {
        rs.updateRowId(arg0, arg1);
    }

    public void updateSQLXML(int arg0, SQLXML arg1) throws SQLException {
        rs.updateSQLXML(arg0, arg1);
    }

    public void updateSQLXML(String arg0, SQLXML arg1) throws SQLException {
        rs.updateSQLXML(arg0, arg1);
    }

    public void updateShort(int arg0, short arg1) throws SQLException {
        rs.updateShort(arg0, arg1);
    }

    public void updateShort(String arg0, short arg1) throws SQLException {
        rs.updateShort(arg0, arg1);
    }

    public void updateString(int arg0, String arg1) throws SQLException {
        rs.updateString(arg0, arg1);
    }

    public void updateString(String arg0, String arg1) throws SQLException {
        rs.updateString(arg0, arg1);
    }

    public void updateTime(int arg0, Time arg1) throws SQLException {
        rs.updateTime(arg0, arg1);
    }

    public void updateTime(String arg0, Time arg1) throws SQLException {
        rs.updateTime(arg0, arg1);
    }

    public void updateTimestamp(int arg0, Timestamp arg1) throws SQLException {
        rs.updateTimestamp(arg0, arg1);
    }

    public void updateTimestamp(String arg0, Timestamp arg1) throws SQLException {
        rs.updateTimestamp(arg0, arg1);
    }

    public boolean wasNull() throws SQLException {
        return rs.wasNull();
    }

    
    
}
