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
import org.davidmoten.rx.jdbc.CallableBuilder.CallableResultSet3;
import org.davidmoten.rx.jdbc.CallableBuilder.CallableResultSet4;
import org.davidmoten.rx.jdbc.CallableBuilder.CallableResultSetN;
import org.davidmoten.rx.jdbc.callable.internal.InParameterPlaceholder;
import org.davidmoten.rx.jdbc.callable.internal.OutParameterPlaceholder;
import org.davidmoten.rx.jdbc.callable.internal.ParameterPlaceholder;
import org.davidmoten.rx.jdbc.tuple.Tuple2;
import org.davidmoten.rx.jdbc.tuple.Tuple3;
import org.davidmoten.rx.jdbc.tuple.Tuple4;
import org.davidmoten.rx.jdbc.tuple.TupleN;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.davidmoten.guavamini.Lists;

import io.reactivex.Emitter;
import io.reactivex.Flowable;
import io.reactivex.Notification;
import io.reactivex.Single;
import io.reactivex.functions.BiConsumer;
import io.reactivex.functions.BiFunction;
import io.reactivex.functions.Consumer;
import io.reactivex.functions.Function;

final class Call {

    private static final Logger log = LoggerFactory.getLogger(Call.class);

    private Call() {
        // prevent instantiation
    }

    /////////////////////////
    // No Parameters
    /////////////////////////

    static Flowable<Integer> createWithZeroOutParameters(Single<Connection> connection, String sql,
            Flowable<List<Object>> parameterGroups, List<ParameterPlaceholder> parameterPlaceholders) {
        return connection.toFlowable()
                .flatMap(con -> Call.<Integer>createWithParameters(con, sql, parameterGroups, parameterPlaceholders,
                        (stmt, parameters) -> createWithZeroOutParameters(stmt, parameters, parameterPlaceholders)))
                .dematerialize();
    }

    private static Single<Integer> createWithZeroOutParameters(NamedCallableStatement stmt, List<Object> parameters,
            List<ParameterPlaceholder> parameterPlaceholders) {
        return Single.fromCallable(() -> {
            CallableStatement st = stmt.stmt;
            execute(stmt, parameters, parameterPlaceholders, 0, st);
            return 1;
        });
    }

    /////////////////////////
    // One Out Parameter
    /////////////////////////

    static <T1> Flowable<Notification<T1>> createWithOneOutParameter(Single<Connection> connection, String sql,
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

    static <T1, T2> Flowable<Notification<Tuple2<T1, T2>>> createWithTwoOutParameters(Single<Connection> connection,
            String sql, Flowable<List<Object>> parameterGroups, List<ParameterPlaceholder> parameterPlaceholders,
            Class<T1> cls1, Class<T2> cls2) {
        return connection.toFlowable().flatMap(con -> createWithParameters(con, sql, parameterGroups,
                parameterPlaceholders,
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
    // Three Out Parameters
    /////////////////////////

    static <T1, T2, T3> Flowable<Notification<Tuple3<T1, T2, T3>>> createWithThreeOutParameters(
            Single<Connection> connection, String sql, Flowable<List<Object>> parameterGroups,
            List<ParameterPlaceholder> parameterPlaceholders, Class<T1> cls1, Class<T2> cls2, Class<T3> cls3) {
        return connection.toFlowable()
                .flatMap(con -> createWithParameters(con, sql, parameterGroups, parameterPlaceholders,
                        (stmt, parameters) -> createWithThreeParameters(stmt, parameters, parameterPlaceholders, cls1,
                                cls2, cls3)));
    }

    private static <T1, T2, T3> Single<Tuple3<T1, T2, T3>> createWithThreeParameters(NamedCallableStatement stmt,
            List<Object> parameters, List<ParameterPlaceholder> parameterPlaceholders, Class<T1> cls1, Class<T2> cls2,
            Class<T3> cls3) {
        return Single.fromCallable(() -> {
            CallableStatement st = stmt.stmt;
            List<PlaceAndType> outs = execute(stmt, parameters, parameterPlaceholders, 3, st);
            T1 o1 = Util.mapObject(st, cls1, outs.get(0).pos, outs.get(0).type);
            T2 o2 = Util.mapObject(st, cls2, outs.get(1).pos, outs.get(1).type);
            T3 o3 = Util.mapObject(st, cls3, outs.get(2).pos, outs.get(2).type);
            return Tuple3.create(o1, o2, o3);
        });
    }

    /////////////////////////
    // Four Out Parameters
    /////////////////////////

    static <T1, T2, T3, T4> Flowable<Notification<Tuple4<T1, T2, T3, T4>>> createWithFourOutParameters(
            Single<Connection> connection, String sql, Flowable<List<Object>> parameterGroups,
            List<ParameterPlaceholder> parameterPlaceholders, Class<T1> cls1, Class<T2> cls2, Class<T3> cls3,
            Class<T4> cls4) {
        return connection.toFlowable()
                .flatMap(con -> createWithParameters(con, sql, parameterGroups, parameterPlaceholders,
                        (stmt, parameters) -> createWithFourParameters(stmt, parameters, parameterPlaceholders, cls1,
                                cls2, cls3, cls4)));
    }

    private static <T1, T2, T3, T4> Single<Tuple4<T1, T2, T3, T4>> createWithFourParameters(NamedCallableStatement stmt,
            List<Object> parameters, List<ParameterPlaceholder> parameterPlaceholders, Class<T1> cls1, Class<T2> cls2,
            Class<T3> cls3, Class<T4> cls4) {
        return Single.fromCallable(() -> {
            CallableStatement st = stmt.stmt;
            List<PlaceAndType> outs = execute(stmt, parameters, parameterPlaceholders, 4, st);
            T1 o1 = Util.mapObject(st, cls1, outs.get(0).pos, outs.get(0).type);
            T2 o2 = Util.mapObject(st, cls2, outs.get(1).pos, outs.get(1).type);
            T3 o3 = Util.mapObject(st, cls3, outs.get(2).pos, outs.get(2).type);
            T4 o4 = Util.mapObject(st, cls4, outs.get(3).pos, outs.get(3).type);
            return Tuple4.create(o1, o2, o3, o4);
        });
    }

    /////////////////////////
    // N Out Parameters
    /////////////////////////

    static Flowable<Notification<TupleN<Object>>> createWithNParameters( //
            Single<Connection> connection, //
            String sql, //
            Flowable<List<Object>> parameterGroups, //
            List<ParameterPlaceholder> parameterPlaceholders, //
            List<Class<?>> outClasses) {
        return connection //
                .toFlowable() //
                .flatMap( //
                        con -> createWithParameters( //
                                con, //
                                sql, //
                                parameterGroups, //
                                parameterPlaceholders, //
                                (stmt, parameters) -> createWithNParameters(stmt, parameters, parameterPlaceholders,
                                        outClasses)));
    }

    private static Single<TupleN<Object>> createWithNParameters( //
            NamedCallableStatement stmt, //
            List<Object> parameters, //
            List<ParameterPlaceholder> parameterPlaceholders, //
            List<Class<?>> outClasses) {
        return Single.fromCallable(() -> {
            CallableStatement st = stmt.stmt;
            List<PlaceAndType> outs = execute(stmt, parameters, parameterPlaceholders, Integer.MAX_VALUE, st);
            Object[] outputs = new Object[outClasses.size()];
            for (int i = 0; i < outClasses.size(); i++) {
                outputs[i] = Util.mapObject(st, outClasses.get(i), outs.get(i).pos, outs.get(i).type);
            }
            return TupleN.create(outputs);
        });
    }

    /////////////////////////
    // One ResultSet
    /////////////////////////

    static <T1> Flowable<Notification<CallableResultSet1<T1>>> createWithOneResultSet(Single<Connection> connection,
            String sql, Flowable<List<Object>> parameterGroups, List<ParameterPlaceholder> parameterPlaceholders,
            Function<? super ResultSet, ? extends T1> f1, int fetchSize) {
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

    static <T1, T2> Flowable<Notification<CallableResultSet2<T1, T2>>> createWithTwoResultSets(
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

    /////////////////////////
    // Three ResultSets
    /////////////////////////

    static <T1, T2, T3> Flowable<Notification<CallableResultSet3<T1, T2, T3>>> createWithThreeResultSets(
            Single<Connection> connection, String sql, Flowable<List<Object>> parameterGroups,
            List<ParameterPlaceholder> parameterPlaceholders, Function<? super ResultSet, ? extends T1> f1,
            Function<? super ResultSet, ? extends T2> f2, Function<? super ResultSet, ? extends T3> f3, int fetchSize) {
        return connection.toFlowable().flatMap(con -> createWithThreeResultSets(con, sql, parameterGroups,
                parameterPlaceholders, f1, f2, f3, fetchSize));
    }

    private static <T1, T2, T3> Flowable<Notification<CallableResultSet3<T1, T2, T3>>> createWithThreeResultSets(
            Connection con, String sql, Flowable<List<Object>> parameterGroups,
            List<ParameterPlaceholder> parameterPlaceholders, Function<? super ResultSet, ? extends T1> f1,
            Function<? super ResultSet, ? extends T2> f2, Function<? super ResultSet, ? extends T3> f3, int fetchSize) {
        Callable<NamedCallableStatement> resourceFactory = () -> Util.prepareCall(con, sql, parameterPlaceholders);
        final Function<NamedCallableStatement, Flowable<Notification<CallableResultSet3<T1, T2, T3>>>> flowableFactory = //
                stmt -> parameterGroups //
                        .flatMap(parameters -> {
                            List<Object> outputValues = executeAndReturnOutputValues(parameterPlaceholders, stmt,
                                    parameters);
                            final Flowable<T1> flowable1 = createFlowable(stmt, f1);
                            stmt.stmt.getMoreResults(Statement.KEEP_CURRENT_RESULT);
                            final Flowable<T2> flowable2 = createFlowable(stmt, f2);
                            stmt.stmt.getMoreResults(Statement.KEEP_CURRENT_RESULT);
                            final Flowable<T3> flowable3 = createFlowable(stmt, f3);
                            return Single.just(
                                    new CallableResultSet3<T1, T2, T3>(outputValues, flowable1, flowable2, flowable3))
                                    .toFlowable();
                        }) //
                        .materialize() //
                        .doOnComplete(() -> Util.commit(stmt.stmt)) //
                        .doOnError(e -> Util.rollback(stmt.stmt));
        Consumer<NamedCallableStatement> disposer = Util::closeCallableStatementAndConnection;
        return Flowable.using(resourceFactory, flowableFactory, disposer, true);
    }
    /////////////////////////
    // Four ResultSets
    /////////////////////////

    static <T1, T2, T3, T4> Flowable<Notification<CallableResultSet4<T1, T2, T3, T4>>> createWithFourResultSets(
            Single<Connection> connection, String sql, Flowable<List<Object>> parameterGroups,
            List<ParameterPlaceholder> parameterPlaceholders, Function<? super ResultSet, ? extends T1> f1,
            Function<? super ResultSet, ? extends T2> f2, Function<? super ResultSet, ? extends T3> f3,
            Function<? super ResultSet, ? extends T4> f4, int fetchSize) {
        return connection.toFlowable().flatMap(con -> createWithFourResultSets(con, sql, parameterGroups,
                parameterPlaceholders, f1, f2, f3, f4, fetchSize));
    }

    private static <T1, T2, T3, T4> Flowable<Notification<CallableResultSet4<T1, T2, T3, T4>>> createWithFourResultSets(
            Connection con, String sql, Flowable<List<Object>> parameterGroups,
            List<ParameterPlaceholder> parameterPlaceholders, Function<? super ResultSet, ? extends T1> f1,
            Function<? super ResultSet, ? extends T2> f2, Function<? super ResultSet, ? extends T3> f3,
            Function<? super ResultSet, ? extends T4> f4, int fetchSize) {
        Callable<NamedCallableStatement> resourceFactory = () -> Util.prepareCall(con, sql, parameterPlaceholders);
        final Function<NamedCallableStatement, Flowable<Notification<CallableResultSet4<T1, T2, T3, T4>>>> flowableFactory = //
                stmt -> parameterGroups //
                        .flatMap(parameters -> {
                            List<Object> outputValues = executeAndReturnOutputValues(parameterPlaceholders, stmt,
                                    parameters);
                            final Flowable<T1> flowable1 = createFlowable(stmt, f1);
                            stmt.stmt.getMoreResults(Statement.KEEP_CURRENT_RESULT);
                            final Flowable<T2> flowable2 = createFlowable(stmt, f2);
                            stmt.stmt.getMoreResults(Statement.KEEP_CURRENT_RESULT);
                            final Flowable<T3> flowable3 = createFlowable(stmt, f3);
                            stmt.stmt.getMoreResults(Statement.KEEP_CURRENT_RESULT);
                            final Flowable<T4> flowable4 = createFlowable(stmt, f4);
                            return Single.just(new CallableResultSet4<T1, T2, T3, T4>(outputValues, flowable1,
                                    flowable2, flowable3, flowable4)).toFlowable();
                        }) //
                        .materialize() //
                        .doOnComplete(() -> Util.commit(stmt.stmt)) //
                        .doOnError(e -> Util.rollback(stmt.stmt));
        Consumer<NamedCallableStatement> disposer = Util::closeCallableStatementAndConnection;
        return Flowable.using(resourceFactory, flowableFactory, disposer, true);
    }

    /////////////////////////
    // N ResultSets
    /////////////////////////

    static Flowable<Notification<CallableResultSetN>> createWithNResultSets(Single<Connection> connection, String sql,
            Flowable<List<Object>> parameterGroups, List<ParameterPlaceholder> parameterPlaceholders,
            List<Function<? super ResultSet, ?>> functions, int fetchSize) {
        return connection.toFlowable().flatMap(
                con -> createWithNResultSets(con, sql, parameterGroups, parameterPlaceholders, functions, fetchSize));
    }

    private static Flowable<Notification<CallableResultSetN>> createWithNResultSets(Connection con, String sql,
            Flowable<List<Object>> parameterGroups, List<ParameterPlaceholder> parameterPlaceholders,
            List<Function<? super ResultSet, ?>> functions, int fetchSize) {
        Callable<NamedCallableStatement> resourceFactory = () -> Util.prepareCall(con, sql, parameterPlaceholders);
        final Function<NamedCallableStatement, Flowable<Notification<CallableResultSetN>>> flowableFactory = //
                stmt -> parameterGroups //
                        .flatMap(parameters -> {
                            List<Object> outputValues = executeAndReturnOutputValues(parameterPlaceholders, stmt,
                                    parameters);
                            List<Flowable<?>> flowables = Lists.newArrayList();
                            int i = 0;
                            do {
                                Function<? super ResultSet, ?> f = functions.get(i);
                                flowables.add(createFlowable(stmt, f));
                                i++;
                            } while (stmt.stmt.getMoreResults(Statement.KEEP_CURRENT_RESULT));
                            return Single.just(new CallableResultSetN(outputValues, flowables)).toFlowable();
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
                if (p instanceof InParameterPlaceholder) {
                    Util.setParameter(ps, j + 1, parameters.get(i));
                    i++;
                }
            }
        } else {
            // TODO
            throw new RuntimeException("named paramters not implemented yet for CallableStatement yet");
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
                outs.add(new PlaceAndType(j + 1, ((OutParameterPlaceholder) p).type()));
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
