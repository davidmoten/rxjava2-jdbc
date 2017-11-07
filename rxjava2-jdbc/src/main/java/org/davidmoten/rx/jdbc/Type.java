package org.davidmoten.rx.jdbc;

public enum Type {

    /**
     * <P>
     * The constant in the Java programming language, sometimes referred to as a
     * type code, that identifies the generic SQL type <code>BIT</code>.
     */
    BIT(-7),

    /**
     * <P>
     * The constant in the Java programming language, sometimes referred to as a
     * type code, that identifies the generic SQL type <code>TINYINT</code>.
     */
    TINYINT(-6),

    /**
     * <P>
     * The constant in the Java programming language, sometimes referred to as a
     * type code, that identifies the generic SQL type <code>SMALLINT</code>.
     */
    SMALLINT(5),

    /**
     * <P>
     * The constant in the Java programming language, sometimes referred to as a
     * type code, that identifies the generic SQL type <code>INTEGER</code>.
     */
    INTEGER(4),

    /**
     * <P>
     * The constant in the Java programming language, sometimes referred to as a
     * type code, that identifies the generic SQL type <code>BIGINT</code>.
     */
    BIGINT(-5),

    /**
     * <P>
     * The constant in the Java programming language, sometimes referred to as a
     * type code, that identifies the generic SQL type <code>FLOAT</code>.
     */
    FLOAT(6),

    /**
     * <P>
     * The constant in the Java programming language, sometimes referred to as a
     * type code, that identifies the generic SQL type <code>REAL</code>.
     */
    REAL(7),

    /**
     * <P>
     * The constant in the Java programming language, sometimes referred to as a
     * type code, that identifies the generic SQL type <code>DOUBLE</code>.
     */
    DOUBLE(8),

    /**
     * <P>
     * The constant in the Java programming language, sometimes referred to as a
     * type code, that identifies the generic SQL type <code>NUMERIC</code>.
     */
    NUMERIC(2),

    /**
     * <P>
     * The constant in the Java programming language, sometimes referred to as a
     * type code, that identifies the generic SQL type <code>DECIMAL</code>.
     */
    DECIMAL(3),

    /**
     * <P>
     * The constant in the Java programming language, sometimes referred to as a
     * type code, that identifies the generic SQL type <code>CHAR</code>.
     */
    CHAR(1),

    /**
     * <P>
     * The constant in the Java programming language, sometimes referred to as a
     * type code, that identifies the generic SQL type <code>VARCHAR</code>.
     */
    VARCHAR(12),

    /**
     * <P>
     * The constant in the Java programming language, sometimes referred to as a
     * type code, that identifies the generic SQL type <code>LONGVARCHAR</code>.
     */
    LONGVARCHAR(-1),

    /**
     * <P>
     * The constant in the Java programming language, sometimes referred to as a
     * type code, that identifies the generic SQL type <code>DATE</code>.
     */
    DATE(91),

    /**
     * <P>
     * The constant in the Java programming language, sometimes referred to as a
     * type code, that identifies the generic SQL type <code>TIME</code>.
     */
    TIME(92),

    /**
     * <P>
     * The constant in the Java programming language, sometimes referred to as a
     * type code, that identifies the generic SQL type <code>TIMESTAMP</code>.
     */
    TIMESTAMP(93),

    /**
     * <P>
     * The constant in the Java programming language, sometimes referred to as a
     * type code, that identifies the generic SQL type <code>BINARY</code>.
     */
    BINARY(-2),

    /**
     * <P>
     * The constant in the Java programming language, sometimes referred to as a
     * type code, that identifies the generic SQL type <code>VARBINARY</code>.
     */
    VARBINARY(-3),

    /**
     * <P>
     * The constant in the Java programming language, sometimes referred to as a
     * type code, that identifies the generic SQL type <code>LONGVARBINARY</code>.
     */
    LONGVARBINARY(-4),

    /**
     * <P>
     * The constant in the Java programming language that identifies the generic SQL
     * value <code>NULL</code>.
     */
    NULL(0),

    /**
     * The constant in the Java programming language that indicates that the SQL
     * type is database-specific and gets mapped to a Java object that can be
     * accessed via the methods <code>getObject</code> and <code>setObject</code>.
     */
    OTHER(1111),

    /**
     * The constant in the Java programming language, sometimes referred to as a
     * type code, that identifies the generic SQL type <code>JAVA_OBJECT</code>.
     * 
     * @since 1.2
     */
    JAVA_OBJECT(2000),

    /**
     * The constant in the Java programming language, sometimes referred to as a
     * type code, that identifies the generic SQL type <code>DISTINCT</code>.
     * 
     * @since 1.2
     */
    DISTINCT(2001),

    /**
     * The constant in the Java programming language, sometimes referred to as a
     * type code, that identifies the generic SQL type <code>STRUCT</code>.
     * 
     * @since 1.2
     */
    STRUCT(2002),

    /**
     * The constant in the Java programming language, sometimes referred to as a
     * type code, that identifies the generic SQL type <code>ARRAY</code>.
     * 
     * @since 1.2
     */
    ARRAY(2003),

    /**
     * The constant in the Java programming language, sometimes referred to as a
     * type code, that identifies the generic SQL type <code>BLOB</code>.
     * 
     * @since 1.2
     */
    BLOB(2004),

    /**
     * The constant in the Java programming language, sometimes referred to as a
     * type code, that identifies the generic SQL type <code>CLOB</code>.
     * 
     * @since 1.2
     */
    CLOB(2005),

    /**
     * The constant in the Java programming language, sometimes referred to as a
     * type code, that identifies the generic SQL type <code>REF</code>.
     * 
     * @since 1.2
     */
    REF(2006),

    /**
     * The constant in the Java programming language, somtimes referred to as a type
     * code, that identifies the generic SQL type <code>DATALINK</code>.
     *
     * @since 1.4
     */
    DATALINK(70),

    /**
     * The constant in the Java programming language, somtimes referred to as a type
     * code, that identifies the generic SQL type <code>BOOLEAN</code>.
     *
     * @since 1.4
     */
    BOOLEAN(16),

    // ------------------------- JDBC 4.0 -----------------------------------

    /**
     * The constant in the Java programming language, sometimes referred to as a
     * type code, that identifies the generic SQL type <code>ROWID</code>
     *
     * @since 1.6
     *
     */
    ROWID(-8),

    /**
     * The constant in the Java programming language, sometimes referred to as a
     * type code, that identifies the generic SQL type <code>NCHAR</code>
     *
     * @since 1.6
     */
    NCHAR(-15),

    /**
     * The constant in the Java programming language, sometimes referred to as a
     * type code, that identifies the generic SQL type <code>NVARCHAR</code>.
     *
     * @since 1.6
     */
    NVARCHAR(-9),

    /**
     * The constant in the Java programming language, sometimes referred to as a
     * type code, that identifies the generic SQL type <code>LONGNVARCHAR</code>.
     *
     * @since 1.6
     */
    LONGNVARCHAR(-16),

    /**
     * The constant in the Java programming language, sometimes referred to as a
     * type code, that identifies the generic SQL type <code>NCLOB</code>.
     *
     * @since 1.6
     */
    NCLOB(2011),

    /**
     * The constant in the Java programming language, sometimes referred to as a
     * type code, that identifies the generic SQL type <code>XML</code>.
     *
     * @since 1.6
     */
    SQLXML(2009),

    // --------------------------JDBC 4.2 -----------------------------

    /**
     * The constant in the Java programming language, sometimes referred to as a
     * type code, that identifies the generic SQL type {@code REF CURSOR}.
     *
     * @since 1.8
     */
    REF_CURSOR(2012),

    /**
     * The constant in the Java programming language, sometimes referred to as a
     * type code, that identifies the generic SQL type {@code TIME WITH TIMEZONE}.
     *
     * @since 1.8
     */
    TIME_WITH_TIMEZONE(2013),

    /**
     * The constant in the Java programming language, sometimes referred to as a
     * type code, that identifies the generic SQL type
     * {@code TIMESTAMP WITH TIMEZONE}.
     *
     * @since 1.8
     */
    TIMESTAMP_WITH_TIMEZONE(2014);

    private final int value;

    Type(int value) {
        this.value = value;
    }

    public int value() {
        return value;
    }
}
