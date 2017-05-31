package org.davidmoten.rx.jdbc;

import java.sql.ResultSet;
import java.sql.SQLException;

import io.reactivex.functions.Function;

public interface ResultSetMapper<T> extends Function<ResultSet, T>{

    @Override
    T apply(ResultSet rs) throws SQLException;
}
