package org.davidmoten.rx.jdbc;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import com.github.davidmoten.guavamini.Preconditions;

import io.reactivex.Flowable;

class ParametersBuilder<T> {

    private static final Flowable<List<Object>> SINGLE_EMPTY_LIST = Flowable.just(Collections.emptyList());

    private final List<Flowable<List<Object>>> parameterGroups = new ArrayList<>();
    // for building up a number of parameters
    private final List<Object> parameterBuffer = new ArrayList<>();
    private final SqlInfo sqlInfo;

    ParametersBuilder(String sql) {
        this.sqlInfo = SqlInfo.parse(sql);
    }

    @SuppressWarnings("unchecked")
    public final T parameterStream(Flowable<?> values) {
        Preconditions.checkNotNull(values);
        if (sqlInfo.numParameters() == 0) {
            parameterListStream(values.map(x -> Collections.emptyList()));
        } else {
            parameterListStream((Flowable<List<?>>) (Flowable<?>) values.buffer(sqlInfo.numParameters()));
        }
        return (T) this;
    }

    @SuppressWarnings("unchecked")
    public final T parameterListStream(Flowable<List<?>> valueLists) {
        Preconditions.checkNotNull(valueLists);
        useAndCloseParameterBuffer();
        parameterGroups.add((Flowable<List<Object>>) (Flowable<?>) valueLists);
        return (T) this;
    }

    @SuppressWarnings("unchecked")
    public final T parameterList(List<Object> values) {
        parameterListStream(Flowable.just(values));
        return (T) this;
    }

    @SuppressWarnings("unchecked")
    public final T parameterList(Object... values) {
        parameterStream(Flowable.fromArray(values) //
                .buffer(sqlInfo.numParameters()));
        return (T) this;
    }

    @SuppressWarnings("unchecked")
    public final T parameter(String name, Object value) {
        Preconditions.checkNotNull(name);
        parameterBuffer.add(new Parameter(name, value));
        return (T) this;
    }

    @SuppressWarnings("unchecked")
    public final T parameters(Object... values) {
        Preconditions.checkNotNull(values);
        if (values.length == 0) {
            // no effect
            return (T) this;
        }
        Preconditions.checkArgument(sqlInfo.numParameters() == 0 || values.length % sqlInfo.numParameters() == 0,
                "number of values should be a multiple of number of parameters in sql: " + sqlInfo.sql());
        Preconditions.checkArgument(Arrays.stream(values)
                .allMatch(o -> sqlInfo.names().isEmpty() || (o instanceof Parameter && ((Parameter) o).hasName())));
        for (Object val : values) {
            parameterBuffer.add(val);
        }
        return (T) this;
    }

    final Flowable<List<Object>> parameterGroupsToFlowable() {
        useAndCloseParameterBuffer();
        Flowable<List<Object>> pg;
        if (parameterGroups.isEmpty()) {
            pg = SINGLE_EMPTY_LIST;
        } else {
            pg = Flowable.concat(parameterGroups);
        }
        return pg;
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
    @SuppressWarnings("unchecked")
    public final T parameterClob(String value) {
        parameters(Database.toSentinelIfNull(value));
        return (T) this;
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
    @SuppressWarnings("unchecked")
    public final T parameterBlob(byte[] bytes) {
        parameters(Database.toSentinelIfNull(bytes));
        return (T) this;
    }

    private void useAndCloseParameterBuffer() {
        // called when about to add stream of parameters or about to call get
        if (!parameterBuffer.isEmpty()) {
            Flowable<List<Object>> p;
            if (sqlInfo.numParameters() > 0) {
                p = Flowable //
                        .fromIterable(new ArrayList<>(parameterBuffer)) //
                        .buffer(sqlInfo.numParameters());
            } else {
                p = Flowable //
                        .fromIterable(new ArrayList<>(parameterBuffer)) //
                        .map(x -> Collections.emptyList());
            }
            parameterGroups.add(p);
            parameterBuffer.clear();
        }
    }

}
