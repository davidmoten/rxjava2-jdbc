package org.davidmoten.rx.jdbc;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import com.github.davidmoten.guavamini.Preconditions;

import io.reactivex.Flowable;

public interface ParameterCollector {
    
    static final Flowable<List<Object>> SINGLE_EMPTY_LIST = Flowable
           .just(Collections.emptyList());
    
    SqlInfo sqlInfo() ;
    
    List<Flowable<List<Object>>> parameterGroups() ;
    
    List<Object> parameterBuffer() ;
    
    @SuppressWarnings("unchecked")
    default void parameterStream(Flowable<?> values) {
        Preconditions.checkNotNull(values);
        if (sqlInfo().numParameters() == 0) {
            parameterListStream(values.map(x -> Collections.emptyList()));
        } else {
            parameterListStream(
                    (Flowable<List<?>>) (Flowable<?>) values.buffer(sqlInfo().numParameters()));
        }
    }

    @SuppressWarnings("unchecked")
    default void parameterListStream(Flowable<List<?>> valueLists) {
        Preconditions.checkNotNull(valueLists);
        _useAndCloseParameterBuffer();
        parameterGroups().add((Flowable<List<Object>>) (Flowable<?>) valueLists);
    }

    default void parameterList(List<Object> values) {
        parameterListStream(Flowable.just(values));
    }

    default void parameterList(Object... values) {
        parameterStream(Flowable.fromArray(values) //
                .buffer(sqlInfo().numParameters()));
    }

    default void parameter(String name, Object value) {
        Preconditions.checkNotNull(name);
        parameterBuffer().add(new Parameter(name, value));
    }

    default void parameters(Object... values) {
        Preconditions.checkNotNull(values);
        if (values.length == 0) {
            // no effect
            return;
        }
        Preconditions.checkArgument(
                sqlInfo().numParameters() == 0 || values.length % sqlInfo().numParameters() == 0,
                "number of values should be a multiple of number of parameters in sql: "
                        + sqlInfo().sql());
        Preconditions.checkArgument(Arrays.stream(values).allMatch(o -> sqlInfo().names().isEmpty()
                || (o instanceof Parameter && ((Parameter) o).hasName())));
        for (Object val : values) {
            parameterBuffer().add(val);
        }
    }

    default Flowable<List<Object>> parameterGroupsToFlowable() {
        _useAndCloseParameterBuffer();
        Flowable<List<Object>> pg;
        if (parameterGroups().isEmpty()) {
            pg = SINGLE_EMPTY_LIST;
        } else {
            pg = Flowable.concat(parameterGroups());
        }
        return pg;
    }

    default void _useAndCloseParameterBuffer() {
        // called when about to add stream of parameters or about to call get
        if (!parameterBuffer().isEmpty()) {
            Flowable<List<Object>> p;
            if (sqlInfo().numParameters() > 0) {
                p = Flowable //
                        .fromIterable(new ArrayList<>(parameterBuffer())) //
                        .buffer(sqlInfo().numParameters());
            } else {
                p = Flowable //
                        .fromIterable(new ArrayList<>(parameterBuffer())) //
                        .map(x -> Collections.emptyList());
            }
            parameterGroups().add(p);
            parameterBuffer().clear();
        }
    }


}
