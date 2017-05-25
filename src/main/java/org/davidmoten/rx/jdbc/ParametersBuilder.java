package org.davidmoten.rx.jdbc;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import com.github.davidmoten.guavamini.Preconditions;

import io.reactivex.Flowable;

final class ParametersBuilder {

    private static final Flowable<List<Object>> SINGLE_EMPTY_LIST = Flowable
            .just(Collections.emptyList());

    // mutable
    private final List<Flowable<List<Object>>> parameterGroups = new ArrayList<>();
    // for building up a number of parameters
    private final List<Object> parameterBuffer = new ArrayList<>();
    private final SqlInfo sqlInfo;

    ParametersBuilder(String sql) {
        this.sqlInfo = SqlInfo.parse(sql);
    }

    @SuppressWarnings("unchecked")
    void parameterStream(Flowable<?> values) {
        Preconditions.checkNotNull(values);
        if (sqlInfo.numParameters() == 0) {
            parameterListStream(values.map(x -> Collections.emptyList()));
        } else {
            parameterListStream(
                    (Flowable<List<?>>) (Flowable<?>) values.buffer(sqlInfo.numParameters()));
        }
    }

    @SuppressWarnings("unchecked")
    void parameterListStream(Flowable<List<?>> valueLists) {
        Preconditions.checkNotNull(valueLists);
        useAndCloseParameterBuffer();
        parameterGroups.add((Flowable<List<Object>>) (Flowable<?>) valueLists);
    }

    void parameterList(List<Object> values) {
        Preconditions.checkNotNull(values);
        parameterListStream(Flowable.just(values));
    }

    void parameterList(Object... values) {
        Preconditions.checkNotNull(values);
        parameterStream(Flowable.fromArray(values).buffer(sqlInfo.numParameters()));
    }

    void parameter(String name, Object value) {
        Preconditions.checkNotNull(name);
        parameterBuffer.add(new Parameter(name, value));
    }

    public void parameters(Object... values) {
        Preconditions.checkNotNull(values);
        if (values.length == 0) {
            // no effect
            return;
        }
        Preconditions.checkArgument(
                sqlInfo.numParameters() == 0 || values.length % sqlInfo.numParameters() == 0,
                "number of values should be a multiple of number of parameters in sql: "
                        + sqlInfo.sql());
        Preconditions.checkArgument(Arrays.stream(values).allMatch(o -> sqlInfo.names().isEmpty()
                || (o instanceof Parameter && ((Parameter) o).hasName())));
        for (Object val : values) {
            parameterBuffer.add(val);
        }
    }

    Flowable<List<Object>> parameterGroupsToFlowable() {
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
