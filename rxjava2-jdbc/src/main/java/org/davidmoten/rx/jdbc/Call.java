package org.davidmoten.rx.jdbc;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;

import org.davidmoten.rx.jdbc.CallableBuilder.CallableResultSet1;
import org.davidmoten.rx.jdbc.CallableBuilder.CallableResultSet2;
import org.davidmoten.rx.jdbc.CallableBuilder.In;
import org.davidmoten.rx.jdbc.CallableBuilder.InOut;
import org.davidmoten.rx.jdbc.CallableBuilder.OutParameterPlaceholder;
import org.davidmoten.rx.jdbc.CallableBuilder.ParameterPlaceholder;
import org.davidmoten.rx.jdbc.tuple.Tuple2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.reactivex.Emitter;
import io.reactivex.Flowable;
import io.reactivex.Notification;
import io.reactivex.Single;
import io.reactivex.functions.BiConsumer;
import io.reactivex.functions.BiFunction;
import io.reactivex.functions.Consumer;
import io.reactivex.functions.Function;

public final class Call {

    private static final Logger log = LoggerFactory.getLogger(Call.class);

    private Call() {
        // prevent instantiation
    }

    /////////////////////////
    // One Out Parameter
    /////////////////////////

    public static <T1> Flowable<Notification<T1>> createWithOneOutParameter(Single<Connection> connection, String sql,
            Flowable<List<Object>> parameterGroups, List<ParameterPlaceholder> parameterPlaceholders, Class<T1> cls) {
        return connection.toFlowable()
                .flatMap(con -> createWithParameters(con, sql, parameterGroups, parameterPlaceholders,
                        (stmt, parameters) -> createWithOneParameter(stmt, parameters, parameterPlaceholders, cls)));
    }

    private static <T> Single<T> createWithOneParameter(NamedCallableStatement stmt, List<Object> parameters,
            List<ParameterPlaceholder> parameterPlaceholders, Class<T> cls1) {
        return Single.fromCallable(() -> {
            CallableStatement st = stmt.stmt;
            List<PlaceAndType> outs = execute(stmt, parameters, parameterPlaceholders, 2, st);
            return Util.mapObject(st, cls1, outs.get(0).pos, outs.get(0).type);
        });
    }

    /////////////////////////
    // Two Out Parameters
    /////////////////////////

    public static <T1, T2> Flowable<Notification<Tuple2<T1, T2>>> createWithTwoOutParameters(
            Single<Connection> connection, String sql, Flowable<List<Object>> parameterGroups,
            List<ParameterPlaceholder> parameterPlaceholders, Class<T1> cls1, Class<T2> cls2) {
        return connection.toFlowable()
                .flatMap(con -> createWithParameters(con, sql, parameterGroups, parameterPlaceholders,
                        (stmt, parameters) -> createWithTwoParameters(stmt, parameters, parameterPlaceholders, cls1, cls2)));
    }

    private static <T1, T2> Single<Tuple2<T1, T2>> createWithTwoParameters(NamedCallableStatement stmt,
            List<Object> parameters, List<ParameterPlaceholder> parameterPlaceholders, Class<T1> cls1, Class<T2> cls2) {
        return Single.fromCallable(() -> {
            CallableStatement st = stmt.stmt;
            List<PlaceAndType> outs = execute(stmt, parameters, parameterPlaceholders, 2, st);
            T1 o1 = Util.mapObject(st, cls1, outs.get(0).pos, outs.get(0).type);
            T2 o2 = Util.mapObject(st, cls2, outs.get(1).pos, outs.get(1).type);
            return Tuple2.create(o1, o2);
        });
    }

    private static final class PlaceAndType {
        final int pos;
        final Type type;

        PlaceAndType(int pos, Type type) {
            this.pos = pos;
            this.type = type;
        }

    }

    /////////////////////////
    // One ResultSet
    /////////////////////////

    public static <T1> Flowable<Notification<CallableResultSet1<T1>>> createWithOneResultSet(
            Single<Connection> connection, String sql, Flowable<List<Object>> parameterGroups,
            List<ParameterPlaceholder> parameterPlaceholders, Function<? super ResultSet, ? extends T1> f1,
            int fetchSize) {
        return connection.toFlowable().flatMap(
                con -> createWithOneResultSet(con, sql, parameterGroups, parameterPlaceholders, f1, fetchSize));
    }

    private static <T1> Flowable<Notification<CallableResultSet1<T1>>> createWithOneResultSet(Connection con,
            String sql, Flowable<List<Object>> parameterGroups, List<ParameterPlaceholder> parameterPlaceholders,
            Function<? super ResultSet, ? extends T1> f1, int fetchSize) {
        log.debug("Update.create {}", sql);
        Callable<NamedCallableStatement> resourceFactory = () -> Util.prepareCall(con, sql, parameterPlaceholders);
        final Function<NamedCallableStatement, Flowable<Notification<CallableResultSet1<T1>>>> flowableFactory = //
                stmt -> parameterGroups //
                        .flatMap(parameters -> {
                            List<Object> outputValues = executeAndReturnOutputValues(parameterPlaceholders, stmt,
                                    parameters);
                            Flowable<T1> flowable1 = createFlowable(stmt, f1);
                            return Single.just(new CallableResultSet1<T1>(outputValues, flowable1)).toFlowable();
                        }) //
                        .materialize() //
                        .doOnComplete(() -> Util.commit(stmt.stmt)) //
                        .doOnError(e -> Util.rollback(stmt.stmt));
        Consumer<NamedCallableStatement> disposer = Util::closeCallableStatementAndConnection;
        return Flowable.using(resourceFactory, flowableFactory, disposer, true);
    }

    /////////////////////////
    // Two ResultSets
    /////////////////////////

    public static <T1, T2> Flowable<Notification<CallableResultSet2<T1, T2>>> createWithTwoResultSets(
            Single<Connection> connection, String sql, Flowable<List<Object>> parameterGroups,
            List<ParameterPlaceholder> parameterPlaceholders, Function<? super ResultSet, ? extends T1> f1,
            Function<? super ResultSet, ? extends T2> f2, int fetchSize) {
        return connection.toFlowable().flatMap(
                con -> createWithTwoResultSets(con, sql, parameterGroups, parameterPlaceholders, f1, f2, fetchSize));
    }

    private static <T1, T2> Flowable<Notification<CallableResultSet2<T1, T2>>> createWithTwoResultSets(Connection con,
            String sql, Flowable<List<Object>> parameterGroups, List<ParameterPlaceholder> parameterPlaceholders,
            Function<? super ResultSet, ? extends T1> f1, Function<? super ResultSet, ? extends T2> f2, int fetchSize) {
        Callable<NamedCallableStatement> resourceFactory = () -> Util.prepareCall(con, sql, parameterPlaceholders);
        final Function<NamedCallableStatement, Flowable<Notification<CallableResultSet2<T1, T2>>>> flowableFactory = //
                stmt -> parameterGroups //
                        .flatMap(parameters -> {
                            List<Object> outputValues = executeAndReturnOutputValues(parameterPlaceholders, stmt,
                                    parameters);
                            final Flowable<T1> flowable1 = createFlowable(stmt, f1);
                            stmt.stmt.getMoreResults(Statement.KEEP_CURRENT_RESULT);
                            final Flowable<T2> flowable2 = createFlowable(stmt, f2);
                            return Single.just(new CallableResultSet2<T1, T2>(outputValues, flowable1, flowable2))
                                    .toFlowable();
                        }) //
                        .materialize() //
                        .doOnComplete(() -> Util.commit(stmt.stmt)) //
                        .doOnError(e -> Util.rollback(stmt.stmt));
        Consumer<NamedCallableStatement> disposer = Util::closeCallableStatementAndConnection;
        return Flowable.using(resourceFactory, flowableFactory, disposer, true);
    }

    ////////////////////////////////////
    // Utilty Methods
    ///////////////////////////////////

    private static <T> Flowable<Notification<T>> createWithParameters(Connection con, String sql,
            Flowable<List<Object>> parameterGroups, List<ParameterPlaceholder> parameterPlaceholders,
            BiFunction<NamedCallableStatement, List<Object>, Single<T>> single) {
        Callable<NamedCallableStatement> resourceFactory = () -> Util.prepareCall(con, sql, parameterPlaceholders);
        final Function<NamedCallableStatement, Flowable<Notification<T>>> flowableFactory = //
                stmt -> parameterGroups //
                        .flatMap(parameters -> single.apply(stmt, parameters).toFlowable()) //
                        .materialize() //
                        .doOnComplete(() -> Util.commit(stmt.stmt)) //
                        .doOnError(e -> Util.rollback(stmt.stmt));
        Consumer<NamedCallableStatement> disposer = Util::closeCallableStatementAndConnection;
        return Flowable.using(resourceFactory, flowableFactory, disposer, true);
    }

    static PreparedStatement setParameters(PreparedStatement ps, List<Object> parameters,
            List<ParameterPlaceholder> parameterPlaceholders, List<String> names) throws SQLException {
        // TODO handle Parameter objects (named)
        if (names.isEmpty()) {
            int i = 0;
            for (int j = 0; j < parameterPlaceholders.size() && i < parameters.size(); j++) {
                ParameterPlaceholder p = parameterPlaceholders.get(j);
                if (p instanceof In || p instanceof InOut) {
                    Util.setParameter(ps, j + 1, parameters.get(i));
                    i++;
                }
            }
        } else {
            // TODO
            throw new RuntimeException("not implemented yet");
            // Util.setNamedParameters(ps, params, names);
        }
        return ps;
    }

    private static List<PlaceAndType> execute(NamedCallableStatement stmt, List<Object> parameters,
            List<ParameterPlaceholder> parameterPlaceholders, int outCount, CallableStatement st) throws SQLException {
        Util.incrementCounter(st.getConnection());
        setParameters(st, parameters, parameterPlaceholders, stmt.names);
        int initialSize = outCount == Integer.MAX_VALUE ? 16 : outCount;
        List<PlaceAndType> outs = new ArrayList<PlaceAndType>(initialSize);
        for (int j = 0; j < parameterPlaceholders.size(); j++) {
            ParameterPlaceholder p = parameterPlaceholders.get(j);
            if (p instanceof OutParameterPlaceholder) {
                outs.add(new PlaceAndType(j + 1, p.type()));
                if (outs.size() == outCount) {
                    break;
                }
            }
        }
        st.execute();
        return outs;
    }

    private static <T> Flowable<T> createFlowable(NamedCallableStatement stmt,
            Function<? super ResultSet, ? extends T> f) throws SQLException {
        ResultSet rsActual = stmt.stmt.getResultSet();
        Callable<ResultSet> initialState = () -> rsActual;
        BiConsumer<ResultSet, Emitter<T>> generator = (rs, emitter) -> {
            log.debug("getting row from ps={}, rs={}", stmt.stmt, rs);
            if (rs.next()) {
                T v = f.apply(rs);
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

    private static List<Object> executeAndReturnOutputValues(List<ParameterPlaceholder> parameterPlaceholders,
            NamedCallableStatement stmt, List<Object> parameters) throws SQLException {
        List<PlaceAndType> outs = execute(stmt, parameters, parameterPlaceholders, Integer.MAX_VALUE, stmt.stmt);
        List<Object> list = new ArrayList<>(outs.size());
        for (PlaceAndType p : outs) {
            // TODO convert to a desired return type?
            list.add(stmt.stmt.getObject(p.pos));
        }
        return list;
    }
}
