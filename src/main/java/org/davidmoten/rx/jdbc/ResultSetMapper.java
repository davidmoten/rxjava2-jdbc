package org.davidmoten.rx.jdbc;

import java.sql.ResultSet;
import java.sql.SQLException;

import io.reactivex.functions.Function;

public interface ResultSetMapper<T> extends Function<ResultSet, T>{

    T apply(ResultSet rs) throws SQLException;
}
