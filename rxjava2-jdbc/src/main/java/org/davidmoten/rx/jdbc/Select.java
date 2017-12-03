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

    static <T> Flowable<T> create(Single<Connection> connections,
            Flowable<List<Object>> parameterGroups, String sql, int fetchSize,
            Function<? super ResultSet, ? extends T> mapper, boolean eagerDispose) {
        return connections //
                .toFlowable() //
                .flatMap(con -> create(con, sql, parameterGroups, fetchSize, mapper, eagerDispose));
    }

    static <T> Flowable<T> create(Connection con, String sql,
            Flowable<List<Object>> parameterGroups, int fetchSize,
            Function<? super ResultSet, T> mapper, boolean eagerDispose) {
        log.debug("Select.create called with con={}", con);
        Callable<NamedPreparedStatement> initialState = () -> Util.prepare(con, fetchSize, sql);
        Function<NamedPreparedStatement, Flowable<T>> observableFactory = ps -> parameterGroups
                .flatMap(parameters -> create(ps.ps, parameters, mapper, ps.names, sql, fetchSize),
                        true, 1);
        Consumer<NamedPreparedStatement> disposer = Util::closePreparedStatementAndConnection;
        return Flowable.using(initialState, observableFactory, disposer, eagerDispose);
    }

    private static <T> Flowable<? extends T> create(PreparedStatement ps, List<Object> parameters,
            Function<? super ResultSet, T> mapper, List<String> names, String sql, int fetchSize) {
        log.debug("parameters={}", parameters);
        log.debug("names={}", names);

        Callable<ResultSet> initialState = () -> {
            List<Parameter> params = Util.toParameters(parameters);
            boolean hasCollection = params.stream().anyMatch(x -> x.isCollection());
            final PreparedStatement ps2;
            if (hasCollection) {
                // create a new prepared statement with the collection ? substituted with
                // ?s to match the size of the collection parameter
                ps2 = Util.prepare(ps.getConnection(), fetchSize, sql, params);
                // now wrap the rs to auto close ps2 because it is single use (the next
                // collection parameter may have a different ordinality so we need to build
                // a new PreparedStatement with a different number of question marks
                // substituted
                return new ResultSetAutoClosesStatement(Util //
                        .setParameters(ps2, params, names) //
                        .executeQuery(), ps2);
            } else {
                // use the current prepared statement (normal re-use)
                ps2 = ps;
                return Util //
                        .setParameters(ps2, params, names) //
                        .executeQuery();
            }
        };
        BiConsumer<ResultSet, Emitter<T>> generator = (rs, emitter) -> {
            log.debug("getting row from ps={}, rs={}", rs.getStatement(), rs);
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
