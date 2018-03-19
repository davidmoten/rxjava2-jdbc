package org.davidmoten.rx.jdbc;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import javax.annotation.Nonnull;

import com.github.davidmoten.guavamini.Preconditions;

import io.reactivex.Flowable;

abstract class ParametersBuilder<T> {

    private static final Flowable<List<Object>> SINGLE_EMPTY_LIST = Flowable.just(Collections.emptyList());

    private final List<Flowable<List<Object>>> parameterGroups = new ArrayList<>();
    // for building up a number of parameters
    private final List<Object> parameterBuffer = new ArrayList<>();
    private final SqlInfo sqlInfo;

    ParametersBuilder(@Nonnull String sql) {
        this.sqlInfo = SqlInfo.parse(sql);
    }

    @SuppressWarnings("unchecked")
    public final T parameterStream(@Nonnull Flowable<?> values) {
        Preconditions.checkNotNull(values);
        if (sqlInfo.numParameters() == 0) {
            parameterListStream(values.map(x -> Collections.emptyList()));
        } else {
            parameterListStream((Flowable<List<?>>) (Flowable<?>) values.buffer(sqlInfo.numParameters()));
        }
        return (T) this;
    }

    @SuppressWarnings("unchecked")
    public final T parameterListStream(@Nonnull Flowable<List<?>> valueLists) {
        Preconditions.checkNotNull(valueLists, "valueLists cannot be null");
        useAndCloseParameterBuffer();
        parameterGroups.add((Flowable<List<Object>>) (Flowable<?>) valueLists);
        return (T) this;
    }

    public final T parameters(@Nonnull List<?> values) {
        return parameterList(values.toArray());
    }

    @SuppressWarnings("unchecked")
    public final T parameter(@Nonnull String name, Object value) {
        Preconditions.checkNotNull(name, "name cannot be null");
        parameterBuffer.add(new Parameter(name, value));
        return (T) this;
    }

    public final T parameter(Object value) {
        return parameters(value);
    }

    public final T parameters(@Nonnull Object... values) {
        return parameterList(values);
    }

    @SuppressWarnings("unchecked")
    private final T parameterList(Object[] values) {
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
            if (val == null) {
                parameterBuffer.add(Parameter.NULL);
            } else {
                parameterBuffer.add(val);
            }
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
