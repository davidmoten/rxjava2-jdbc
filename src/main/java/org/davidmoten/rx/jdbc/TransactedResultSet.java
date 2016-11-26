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
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.Map;

public class TransactedResultSet {

    private final ResultSet rs;
    private final TransactedConnection con;

    public TransactedResultSet(TransactedConnection con, ResultSet rs) {
        this.con = con;
        this.rs = rs;
    }

    public boolean absolute(int arg0) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void afterLast() throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void beforeFirst() throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void cancelRowUpdates() throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void clearWarnings() throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void close() throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void deleteRow() throws SQLException {
        throw new UnsupportedOperationException();
    }

    public int findColumn(String arg0) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public boolean first() throws SQLException {
        throw new UnsupportedOperationException();
    }

    public Array getArray(int arg0) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public Array getArray(String arg0) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public InputStream getAsciiStream(int arg0) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public InputStream getAsciiStream(String arg0) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public BigDecimal getBigDecimal(int arg0, int arg1) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public BigDecimal getBigDecimal(int arg0) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public BigDecimal getBigDecimal(String arg0, int arg1) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public BigDecimal getBigDecimal(String arg0) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public InputStream getBinaryStream(int arg0) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public InputStream getBinaryStream(String arg0) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public Blob getBlob(int arg0) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public Blob getBlob(String arg0) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public boolean getBoolean(int arg0) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public boolean getBoolean(String arg0) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public byte getByte(int arg0) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public byte getByte(String arg0) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public byte[] getBytes(int arg0) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public byte[] getBytes(String arg0) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public Reader getCharacterStream(int arg0) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public Reader getCharacterStream(String arg0) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public Clob getClob(int arg0) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public Clob getClob(String arg0) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public int getConcurrency() throws SQLException {
        throw new UnsupportedOperationException();
    }

    public String getCursorName() throws SQLException {
        throw new UnsupportedOperationException();
    }

    public Date getDate(int arg0, Calendar arg1) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public Date getDate(int arg0) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public Date getDate(String arg0, Calendar arg1) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public Date getDate(String arg0) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public double getDouble(int arg0) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public double getDouble(String arg0) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public int getFetchDirection() throws SQLException {
        throw new UnsupportedOperationException();
    }

    public int getFetchSize() throws SQLException {
        throw new UnsupportedOperationException();
    }

    public float getFloat(int arg0) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public float getFloat(String arg0) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public int getHoldability() throws SQLException {
        throw new UnsupportedOperationException();
    }

    public int getInt(int arg0) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public int getInt(String arg0) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public long getLong(int arg0) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public long getLong(String arg0) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public ResultSetMetaData getMetaData() throws SQLException {
        throw new UnsupportedOperationException();
    }

    public Reader getNCharacterStream(int arg0) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public Reader getNCharacterStream(String arg0) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public NClob getNClob(int arg0) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public NClob getNClob(String arg0) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public String getNString(int arg0) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public String getNString(String arg0) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public <T> T getObject(int arg0, Class<T> arg1) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public Object getObject(int arg0, Map<String, Class<?>> arg1) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public Object getObject(int arg0) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public <T> T getObject(String arg0, Class<T> arg1) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public Object getObject(String arg0, Map<String, Class<?>> arg1) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public Object getObject(String arg0) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public Ref getRef(int arg0) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public Ref getRef(String arg0) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public int getRow() throws SQLException {
        throw new UnsupportedOperationException();
    }

    public RowId getRowId(int arg0) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public RowId getRowId(String arg0) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public SQLXML getSQLXML(int arg0) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public SQLXML getSQLXML(String arg0) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public short getShort(int arg0) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public short getShort(String arg0) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public Statement getStatement() throws SQLException {
        throw new UnsupportedOperationException();
    }

    public String getString(int arg0) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public String getString(String arg0) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public Time getTime(int arg0, Calendar arg1) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public Time getTime(int arg0) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public Time getTime(String arg0, Calendar arg1) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public Time getTime(String arg0) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public Timestamp getTimestamp(int arg0, Calendar arg1) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public Timestamp getTimestamp(int arg0) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public Timestamp getTimestamp(String arg0, Calendar arg1) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public Timestamp getTimestamp(String arg0) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public int getType() throws SQLException {
        throw new UnsupportedOperationException();
    }

    public URL getURL(int arg0) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public URL getURL(String arg0) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public InputStream getUnicodeStream(int arg0) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public InputStream getUnicodeStream(String arg0) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public SQLWarning getWarnings() throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void insertRow() throws SQLException {
        throw new UnsupportedOperationException();
    }

    public boolean isAfterLast() throws SQLException {
        throw new UnsupportedOperationException();
    }

    public boolean isBeforeFirst() throws SQLException {
        throw new UnsupportedOperationException();
    }

    public boolean isClosed() throws SQLException {
        throw new UnsupportedOperationException();
    }

    public boolean isFirst() throws SQLException {
        throw new UnsupportedOperationException();
    }

    public boolean isLast() throws SQLException {
        throw new UnsupportedOperationException();
    }

    public boolean isWrapperFor(Class<?> arg0) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public boolean last() throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void moveToCurrentRow() throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void moveToInsertRow() throws SQLException {
        throw new UnsupportedOperationException();
    }

    public boolean next() throws SQLException {
        throw new UnsupportedOperationException();
    }

    public boolean previous() throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void refreshRow() throws SQLException {
        throw new UnsupportedOperationException();
    }

    public boolean relative(int arg0) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public boolean rowDeleted() throws SQLException {
        throw new UnsupportedOperationException();
    }

    public boolean rowInserted() throws SQLException {
        throw new UnsupportedOperationException();
    }

    public boolean rowUpdated() throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void setFetchDirection(int arg0) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void setFetchSize(int arg0) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public <T> T unwrap(Class<T> arg0) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void updateArray(int arg0, Array arg1) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void updateArray(String arg0, Array arg1) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void updateAsciiStream(int arg0, InputStream arg1, int arg2) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void updateAsciiStream(int arg0, InputStream arg1, long arg2) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void updateAsciiStream(int arg0, InputStream arg1) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void updateAsciiStream(String arg0, InputStream arg1, int arg2) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void updateAsciiStream(String arg0, InputStream arg1, long arg2) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void updateAsciiStream(String arg0, InputStream arg1) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void updateBigDecimal(int arg0, BigDecimal arg1) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void updateBigDecimal(String arg0, BigDecimal arg1) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void updateBinaryStream(int arg0, InputStream arg1, int arg2) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void updateBinaryStream(int arg0, InputStream arg1, long arg2) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void updateBinaryStream(int arg0, InputStream arg1) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void updateBinaryStream(String arg0, InputStream arg1, int arg2) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void updateBinaryStream(String arg0, InputStream arg1, long arg2) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void updateBinaryStream(String arg0, InputStream arg1) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void updateBlob(int arg0, Blob arg1) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void updateBlob(int arg0, InputStream arg1, long arg2) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void updateBlob(int arg0, InputStream arg1) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void updateBlob(String arg0, Blob arg1) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void updateBlob(String arg0, InputStream arg1, long arg2) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void updateBlob(String arg0, InputStream arg1) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void updateBoolean(int arg0, boolean arg1) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void updateBoolean(String arg0, boolean arg1) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void updateByte(int arg0, byte arg1) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void updateByte(String arg0, byte arg1) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void updateBytes(int arg0, byte[] arg1) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void updateBytes(String arg0, byte[] arg1) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void updateCharacterStream(int arg0, Reader arg1, int arg2) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void updateCharacterStream(int arg0, Reader arg1, long arg2) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void updateCharacterStream(int arg0, Reader arg1) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void updateCharacterStream(String arg0, Reader arg1, int arg2) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void updateCharacterStream(String arg0, Reader arg1, long arg2) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void updateCharacterStream(String arg0, Reader arg1) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void updateClob(int arg0, Clob arg1) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void updateClob(int arg0, Reader arg1, long arg2) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void updateClob(int arg0, Reader arg1) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void updateClob(String arg0, Clob arg1) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void updateClob(String arg0, Reader arg1, long arg2) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void updateClob(String arg0, Reader arg1) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void updateDate(int arg0, Date arg1) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void updateDate(String arg0, Date arg1) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void updateDouble(int arg0, double arg1) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void updateDouble(String arg0, double arg1) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void updateFloat(int arg0, float arg1) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void updateFloat(String arg0, float arg1) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void updateInt(int arg0, int arg1) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void updateInt(String arg0, int arg1) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void updateLong(int arg0, long arg1) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void updateLong(String arg0, long arg1) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void updateNCharacterStream(int arg0, Reader arg1, long arg2) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void updateNCharacterStream(int arg0, Reader arg1) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void updateNCharacterStream(String arg0, Reader arg1, long arg2) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void updateNCharacterStream(String arg0, Reader arg1) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void updateNClob(int arg0, NClob arg1) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void updateNClob(int arg0, Reader arg1, long arg2) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void updateNClob(int arg0, Reader arg1) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void updateNClob(String arg0, NClob arg1) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void updateNClob(String arg0, Reader arg1, long arg2) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void updateNClob(String arg0, Reader arg1) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void updateNString(int arg0, String arg1) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void updateNString(String arg0, String arg1) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void updateNull(int arg0) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void updateNull(String arg0) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void updateObject(int arg0, Object arg1, int arg2) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void updateObject(int arg0, Object arg1) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void updateObject(String arg0, Object arg1, int arg2) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void updateObject(String arg0, Object arg1) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void updateRef(int arg0, Ref arg1) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void updateRef(String arg0, Ref arg1) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void updateRow() throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void updateRowId(int arg0, RowId arg1) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void updateRowId(String arg0, RowId arg1) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void updateSQLXML(int arg0, SQLXML arg1) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void updateSQLXML(String arg0, SQLXML arg1) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void updateShort(int arg0, short arg1) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void updateShort(String arg0, short arg1) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void updateString(int arg0, String arg1) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void updateString(String arg0, String arg1) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void updateTime(int arg0, Time arg1) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void updateTime(String arg0, Time arg1) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void updateTimestamp(int arg0, Timestamp arg1) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void updateTimestamp(String arg0, Timestamp arg1) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public boolean wasNull() throws SQLException {
        throw new UnsupportedOperationException();
    }

}
