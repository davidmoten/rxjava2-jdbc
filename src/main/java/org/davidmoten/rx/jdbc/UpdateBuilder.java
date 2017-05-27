package org.davidmoten.rx.jdbc;

import java.sql.Connection;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

import com.github.davidmoten.guavamini.Preconditions;

import io.reactivex.Flowable;

public final class UpdateBuilder {

    static final int DEFAULT_BATCH_SIZE = 1;

    final String sql;
    final Flowable<Connection> connections;
    private final Database db;
    private final ParametersBuilder parameters;
    private List<Flowable<?>> dependsOn;
    int batchSize = DEFAULT_BATCH_SIZE;


    public UpdateBuilder(String sql, Flowable<Connection> connections, Database db) {
        this.sql = sql;
        this.connections = connections;
        this.db = db;
        this.parameters = new ParametersBuilder(sql);
    }
    
    public UpdateBuilder parameterStream(Flowable<?> values) {
        parameters.parameterStream(values);
        return this;
    }

    public UpdateBuilder parameterListStream(Flowable<List<?>> valueLists) {
        parameters.parameterListStream(valueLists);
        return this;
    }

    public UpdateBuilder parameterList(List<Object> values) {
        parameters.parameterList(values);
        return this;
    }

    public UpdateBuilder parameterList(Object... values) {
        parameters.parameterList(values);
        return this;
    }

    public UpdateBuilder parameter(String name, Object value) {
        parameters.parameter(name, value);
        return this;
    }

    public UpdateBuilder parameters(Object... values) {
        parameters.parameters(values);
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
    public UpdateBuilder parameterClob(String value) {
        parameters(Database.toSentinelIfNull(value));
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
    public UpdateBuilder parameterBlob(byte[] bytes) {
        parameters(Database.toSentinelIfNull(bytes));
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
    public UpdateBuilder dependsOn(Flowable<?> dependency) {
        if (this.dependsOn == null) {
            this.dependsOn = new ArrayList<Flowable<?>>();

        }
        this.dependsOn.add(dependency);
        return this;
    }

    public UpdateBuilder batchSize(int batchSize) {
        this.batchSize = batchSize;
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
        Preconditions.checkArgument(batchSize == 1,
                "Cannot return generated keys if batchSize > 1");
        return new ReturnGeneratedKeysBuilder(this);
    }

    public Flowable<Integer> counts() {
        return Update.create(connections.firstOrError(), parameters.parameterGroupsToFlowable(), sql, batchSize);
    }
    
    Flowable<List<Object>> parameterGroupsToFlowable() {
        return parameters.parameterGroupsToFlowable();
    }

    public TransactedUpdateBuilder transacted() {
        return new TransactedUpdateBuilder(this, db);
    }

}
