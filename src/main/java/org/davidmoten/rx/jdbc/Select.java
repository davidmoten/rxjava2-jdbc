package org.davidmoten.rx.jdbc;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.List;
import java.util.concurrent.Callable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.reactivex.Emitter;
import io.reactivex.Flowable;
import io.reactivex.Single;
import io.reactivex.functions.BiConsumer;
import io.reactivex.functions.Consumer;
import io.reactivex.functions.Function;

final class Select {

    private Select() {
        // prevent instantiation
    }

    private static final Logger log = LoggerFactory.getLogger(Select.class);

    public static <T> Flowable<T> create(Single<Connection> connections, Flowable<List<Object>> parameterGroups,
            String sql, int fetchSize, Function<? super ResultSet, ? extends T> mapper) {
        return connections //
                .toFlowable() //
                .flatMap(con -> create(con, sql, parameterGroups, fetchSize, mapper));
    }

    private static <T> Flowable<T> create(Connection con, String sql, Flowable<List<Object>> parameterGroups,
            int fetchSize, Function<? super ResultSet, T> mapper) {
        log.debug("create called with con={}", con);
        Callable<NamedPreparedStatement> initialState = () -> Util.prepare(con, fetchSize, sql);
        Function<NamedPreparedStatement, Flowable<T>> observableFactory = ps -> parameterGroups
                .flatMap(parameters -> create(con, ps.ps, parameters, mapper, ps.names), true, 1);
        Consumer<NamedPreparedStatement> disposer = Util::closePreparedStatementAndConnection;
        return Flowable.using(initialState, observableFactory, disposer, true);
    }

    private static <T> Flowable<? extends T> create(Connection con, PreparedStatement ps, List<Object> parameters,
            Function<? super ResultSet, T> mapper, List<String> names) {
        log.debug("parameters={}", parameters);
        log.debug("names={}", names);
        Callable<ResultSet> initialState = () -> Util //
                .setParameters(ps, parameters, names) //
                .executeQuery();
        BiConsumer<ResultSet, Emitter<T>> generator = (rs, emitter) -> {
            log.debug("getting row from ps={}, rs={}", ps, rs);
            if (rs.next()) {
                T v = mapper.apply(rs);
                log.debug("emitting {}", v);
                emitter.onNext(v);
            } else {
                log.debug("completed");
                emitter.onComplete();
            }
        };
        Consumer<ResultSet> disposeState = Util::closeSilently;
        return Flowable.generate(initialState, generator, disposeState);
    }

}
