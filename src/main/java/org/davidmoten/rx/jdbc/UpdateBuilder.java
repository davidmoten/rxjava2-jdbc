package org.davidmoten.rx.jdbc;

import java.sql.Connection;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

import com.github.davidmoten.guavamini.Preconditions;

import io.reactivex.Flowable;
import io.reactivex.functions.Function;

public final class UpdateBuilder {
    
    private static final int DEFAULT_BATCH_SIZE = 1;

    private final String sql;
    private final Flowable<Connection> connections;
    private Flowable<Parameter> parameters;
    private List<Flowable<?>> dependsOn;
    private int batchSize = DEFAULT_BATCH_SIZE;

    public UpdateBuilder(String sql, Flowable<Connection> connections) {
        this.sql = sql;
        this.connections = connections;
    }

    /**
     * Appends the given parameters to the parameter list for the query. If
     * there are more parameters than required for one execution of the query
     * then more than one execution of the query will occur.
     * 
     * @param parameters
     * @return this
     */
    public <T> UpdateBuilder parameters(Flowable<T> parameters) {
        //TODO
        throw new UnsupportedOperationException();
    }

    /**
     * Appends the given parameter values to the parameter list for the query.
     * If there are more parameters than required for one execution of the query
     * then more than one execution of the query will occur.
     * 
     * @param objects
     * @return this
     */
    public UpdateBuilder parameters(Object... objects) {
        Flowable<Parameter> p = Flowable.fromArray(objects).map(TO_PARAMETER);
        if (parameters == null) {
            parameters = p;
        } else {
            this.parameters = Flowable.concat(parameters, p);
        }
        return this;
    }

    /**
     * Appends a parameter to the parameter list for the query. If there are
     * more parameters than required for one execution of the query then more
     * than one execution of the query will occur.
     * 
     * @param value
     * @return this
     */
    public UpdateBuilder parameter(Object value) {
        return parameters(Flowable.just(value));
    }

    /**
     * Sets a named parameter. If name is null throws a
     * {@link NullPointerException}. If value is instance of Observable then
     * throws an {@link IllegalArgumentException}.
     * 
     * @param name
     *            the parameter name. Cannot be null.
     * @param value
     *            the parameter value
     */
    public UpdateBuilder parameter(String name, Object value) {
        //TODO
        throw new UnsupportedOperationException();
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
        parameter(Database.toSentinelIfNull(value));
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
        parameter(Database.toSentinelIfNull(bytes));
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
     * {@link ResultSet}. Not all jdbc drivers support this functionality
     * and some have limitations in their support (h2 for instance only
     * returns the last generated key when multiple inserts happen in the
     * one statement).
     * 
     * @return a builder used to specify how to process the generated keys
     *         ResultSet
     */
    public ReturnGeneratedKeysBuilder returnGeneratedKeys() {
        Preconditions.checkArgument(batchSize == 1,
                "Cannot return generated keys if batchSize > 1");
        return new ReturnGeneratedKeysBuilder(this);
    }
    
    private static final Function<Object, Parameter> TO_PARAMETER = new Function<Object, Parameter>() {

        @Override
        public Parameter apply(Object parameter) {
            Preconditions.checkArgument(!(parameter instanceof Parameter));
            return new Parameter(parameter);
        }
    };

}
