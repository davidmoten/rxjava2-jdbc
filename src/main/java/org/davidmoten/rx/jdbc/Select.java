package org.davidmoten.rx.jdbc;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.List;
import java.util.concurrent.Callable;

import io.reactivex.Emitter;
import io.reactivex.Flowable;
import io.reactivex.functions.BiConsumer;
import io.reactivex.functions.Consumer;
import io.reactivex.functions.Function;

public enum Select {
    ;

    public static <T> Flowable<T> create(Flowable<Connection> connections,
            Flowable<List<Object>> parameters, String sql, Function<? super ResultSet, T> mapper) {
        return connections //
                .firstOrError() //
                .toFlowable() //
                .flatMap(con -> create(con, sql, parameters, mapper));
    }

    private static <T> Flowable<T> create(Connection con, String sql,
            Flowable<List<Object>> parameterGroups, Function<? super ResultSet, T> mapper) {
        Callable<NamedPreparedStatement> initialState = () -> Util.prepare(con, sql);
        Function<NamedPreparedStatement, Flowable<T>> observableFactory = ps -> parameterGroups
                .flatMap(parameters -> create(con, ps.ps, parameters, mapper, ps.names), true, 1) //
                ;
        Consumer<NamedPreparedStatement> disposer = Util::closePreparedStatementAndConnection;
        return Flowable.using(initialState, observableFactory, disposer, true);
    }

    private static <T> Flowable<? extends T> create(Connection con, PreparedStatement ps,
            List<Object> parameters, Function<? super ResultSet, T> mapper, List<String> names) {
        Callable<ResultSet> initialState = () -> Util //
                .setParameters(ps, parameters, names) //
                .executeQuery();
        BiConsumer<ResultSet, Emitter<T>> generator = (rs, emitter) -> {
            if (rs.next()) {
                emitter.onNext(mapper.apply(rs));
            } else {
                emitter.onComplete();
            }
        };
        Consumer<ResultSet> disposeState = Util::closeSilently;
        return Flowable.generate(initialState, generator, disposeState);
    }

}
