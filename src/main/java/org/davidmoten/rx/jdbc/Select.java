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

public class Select {

    public static <T> Flowable<T> create(Flowable<Connection> connections, Flowable<List<Object>> parameters,
            String sql, Function<? super ResultSet, T> mapper) {
        return connections //
                .firstOrError() //
                .toFlowable() //
                .flatMap(con -> create(con, sql, parameters, mapper));
    }

    private static <T> Flowable<T> create(Connection con, String sql, Flowable<List<Object>> parameterGroups,
            Function<? super ResultSet, T> mapper) {
        Callable<PreparedStatement> initialState = () -> con.prepareStatement(sql);
        Function<PreparedStatement, Flowable<T>> observableFactory = ps -> parameterGroups
                .flatMap(parameters -> create(con, ps, parameters, mapper), true, 1);
        Consumer<PreparedStatement> disposer = ps -> ps.close();
        return Flowable.using(initialState, observableFactory, disposer, true);
    }

    private static <T> Flowable<? extends T> create(Connection con, PreparedStatement ps, List<Object> parameters,
            Function<? super ResultSet, T> mapper) {
        Callable<ResultSet> initialState = () -> Util //
                .setParameters(ps, parameters) //
                .getResultSet();
        BiConsumer<ResultSet, Emitter<T>> generator = (rs, emitter) -> {
            if (rs.next()) {
                emitter.onNext(mapper.apply(rs));
            } else {
                emitter.onComplete();
            }
        };
        Consumer<ResultSet> disposeState = rs -> rs.close();
        return Flowable.generate(initialState, generator, disposeState);
    }

}
