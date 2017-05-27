package org.davidmoten.rx.jdbc;

import java.sql.ResultSet;
import java.util.List;

import io.reactivex.Flowable;

public final class TransactedUpdateBuilder {

    private final UpdateBuilder b;
    private final Database db;

    public TransactedUpdateBuilder(UpdateBuilder b, Database db) {
        this.b = b;
        this.db = db;
    }

    public TransactedUpdateBuilder parameterStream(Flowable<?> values) {
        b.parameterStream(values);
        return this;
    }

    public TransactedUpdateBuilder parameterListStream(Flowable<List<?>> valueLists) {
        b.parameterListStream(valueLists);
        return this;
    }

    public TransactedUpdateBuilder parameterList(List<Object> values) {
        b.parameterList(values);
        return this;
    }

    public TransactedUpdateBuilder parameterList(Object... values) {
        b.parameterList(values);
        return this;
    }

    public TransactedUpdateBuilder parameter(String name, Object value) {
        b.parameter(name, value);
        return this;
    }

    public TransactedUpdateBuilder parameters(Object... values) {
        b.parameters(values);
        return this;
    }

    /**
     * Appends a parameter to the parameter list for the query for a CLOB
     * parameter and handles null appropriately. If there are more parameters
     * than required for one execution of the query then more than one execution
     * of the query will occur.
     * 
     * @param value
     *            the string to insert in the CLOB column
     * @return this
     */
    public TransactedUpdateBuilder parameterClob(String value) {
        b.parameterClob(value);
        return this;
    }

    /**
     * Appends a parameter to the parameter list for the query for a CLOB
     * parameter and handles null appropriately. If there are more parameters
     * than required for one execution of the query then more than one execution
     * of the query will occur.
     * 
     * @param value
     * @return this
     */
    public TransactedUpdateBuilder parameterBlob(byte[] bytes) {
        b.parameterBlob(bytes);
        return this;
    }

    /**
     * Appends a dependency to the dependencies that have to complete their
     * emitting before the query is executed.
     * 
     * @param dependency
     *            dependency that must complete before the Flowable built by
     *            this subscribes.
     * @return this this
     */
    public TransactedUpdateBuilder dependsOn(Flowable<?> dependency) {
        b.dependsOn(dependency);
        return this;
    }

    public TransactedUpdateBuilder batchSize(int batchSize) {
        b.batchSize(batchSize);
        return this;
    }

    /**
     * Returns a builder used to specify how to process the generated keys
     * {@link ResultSet}. Not all jdbc drivers support this functionality and
     * some have limitations in their support (h2 for instance only returns the
     * last generated key when multiple inserts happen in the one statement).
     * 
     * @return a builder used to specify how to process the generated keys
     *         ResultSet
     */
    public ReturnGeneratedKeysBuilder returnGeneratedKeys() {
        return b.returnGeneratedKeys();
    }

    public Flowable<Integer> counts() {
        return b.counts();
    }
}
