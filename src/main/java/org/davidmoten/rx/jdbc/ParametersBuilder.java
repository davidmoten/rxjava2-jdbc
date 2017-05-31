package org.davidmoten.rx.jdbc;

import java.util.ArrayList;
import java.util.List;

import io.reactivex.Flowable;

final class ParametersBuilder implements ParameterCollector {

    // mutable
    private final List<Flowable<List<Object>>> parameterGroups = new ArrayList<>();
    // for building up a number of parameters
    private final List<Object> parameterBuffer = new ArrayList<>();
    private final SqlInfo sqlInfo;

    ParametersBuilder(String sql) {
        this.sqlInfo = SqlInfo.parse(sql);
    }
    
    public SqlInfo sqlInfo() {
        return sqlInfo;
    }
    
    public List<Flowable<List<Object>>> parameterGroups() {
        return parameterGroups;
    }
    
    public List<Object> parameterBuffer() {
        return parameterBuffer;
    }
    
}
