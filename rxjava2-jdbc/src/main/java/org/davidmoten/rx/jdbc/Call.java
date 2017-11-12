package org.davidmoten.rx.jdbc;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;

import org.davidmoten.rx.jdbc.CallableBuilder.In;
import org.davidmoten.rx.jdbc.CallableBuilder.InOut;
import org.davidmoten.rx.jdbc.CallableBuilder.OutParameterPlaceholder;
import org.davidmoten.rx.jdbc.CallableBuilder.ParameterPlaceholder;
import org.davidmoten.rx.jdbc.tuple.Tuple2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.reactivex.Flowable;
import io.reactivex.Notification;
import io.reactivex.Single;
import io.reactivex.functions.Consumer;
import io.reactivex.functions.Function;

public final class Call {

    private static final Logger log = LoggerFactory.getLogger(Call.class);

    private Call() {
        // prevent instantiation
    }

    public static <T1> Flowable<Notification<T1>> createWithOneParameter(Connection con, String sql,
            Flowable<List<Object>> parameterGroups,
            List<ParameterPlaceholder> parameterPlaceholders, Class<T1> cls) {
        log.debug("Update.create {}", sql);
        Callable<NamedCallableStatement> resourceFactory = () -> Util.prepareCall(con, sql,
                parameterPlaceholders);
        final Function<NamedCallableStatement, Flowable<Notification<T1>>> flowableFactory = //
                stmt -> parameterGroups //
                        .flatMap(parameters -> createWithOneParameter(stmt, parameters,
                                parameterPlaceholders, cls).toFlowable()) //
                        .materialize() //
                        .doOnComplete(() -> Util.commit(stmt.stmt)) //
                        .doOnError(e -> Util.rollback(stmt.stmt));
        Consumer<NamedCallableStatement> disposer = Util::closeCallableStatementAndConnection;
        return Flowable.using(resourceFactory, flowableFactory, disposer, true);
    }

    private static <T> Single<T> createWithOneParameter(NamedCallableStatement stmt,
            List<Object> parameters, List<ParameterPlaceholder> parameterPlaceholders,
            Class<T> cls) {
        return Single.fromCallable(() -> {
            CallableStatement st = stmt.stmt;
            Util.incrementCounter(st.getConnection());
            setParameters(st, parameters, parameterPlaceholders, stmt.names);
            int pos = 0;
            ParameterPlaceholder p = null;
            for (int j = 0; j < parameterPlaceholders.size(); j++) {
                p = parameterPlaceholders.get(j);
                if (p instanceof OutParameterPlaceholder) {
                    pos = j + 1;
                    break;
                }
            }
            st.execute();
            return Util.mapObject(st, cls, pos, p.type());
        });
    }

    public static <T1> Flowable<Notification<T1>> createWithOneOutParameter(
            Single<Connection> connection, String sql, Flowable<List<Object>> parameterGroups,
            List<ParameterPlaceholder> parameterPlaceholders, Class<T1> cls) {
        return connection.toFlowable().flatMap(con -> createWithOneParameter(con, sql,
                parameterGroups, parameterPlaceholders, cls));
    }

    // TWO

    public static <T1, T2> Flowable<Notification<Tuple2<T1, T2>>> createWithTwoOutParameters(
            Single<Connection> connection, String sql, Flowable<List<Object>> parameterGroups,
            List<ParameterPlaceholder> parameterPlaceholders, Class<T1> cls1, Class<T2> cls2) {
        return connection.toFlowable().flatMap(con -> createWithTwoParameters(con, sql,
                parameterGroups, parameterPlaceholders, cls1, cls2));
    }

    private static <T1, T2> Flowable<Notification<Tuple2<T1, T2>>> createWithTwoParameters(
            Connection con, String sql, Flowable<List<Object>> parameterGroups,
            List<ParameterPlaceholder> parameterPlaceholders, Class<T1> cls1, Class<T2> cls2) {
        log.debug("Update.create {}", sql);
        Callable<NamedCallableStatement> resourceFactory = () -> Util.prepareCall(con, sql,
                parameterPlaceholders);
        final Function<NamedCallableStatement, Flowable<Notification<Tuple2<T1, T2>>>> flowableFactory = //
                stmt -> parameterGroups //
                        .flatMap(parameters -> createWithTwoParameters(stmt, parameters,
                                parameterPlaceholders, cls1, cls2).toFlowable()) //
                        .materialize() //
                        .doOnComplete(() -> Util.commit(stmt.stmt)) //
                        .doOnError(e -> Util.rollback(stmt.stmt));
        Consumer<NamedCallableStatement> disposer = Util::closeCallableStatementAndConnection;
        return Flowable.using(resourceFactory, flowableFactory, disposer, true);
    }

    private static <T1, T2> Single<Tuple2<T1, T2>> createWithTwoParameters(
            NamedCallableStatement stmt, List<Object> parameters,
            List<ParameterPlaceholder> parameterPlaceholders, Class<T1> cls1, Class<T2> cls2) {
        return Single.fromCallable(() -> {
            CallableStatement st = stmt.stmt;
            List<PlaceAndType> outs = execute(stmt, parameters, parameterPlaceholders, 2, st);
            T1 o1 = Util.mapObject(st, cls1, outs.get(0).pos, outs.get(0).type);
            T2 o2 = Util.mapObject(st, cls2, outs.get(1).pos, outs.get(1).type);
            return Tuple2.create(o1, o2);
        });
    }

    private static List<PlaceAndType> execute(NamedCallableStatement stmt, List<Object> parameters,
            List<ParameterPlaceholder> parameterPlaceholders, int count, CallableStatement st)
            throws SQLException {
        Util.incrementCounter(st.getConnection());
        setParameters(st, parameters, parameterPlaceholders, stmt.names);
        List<PlaceAndType> outs = new ArrayList<PlaceAndType>(count);
        for (int j = 0; j < parameterPlaceholders.size(); j++) {
            ParameterPlaceholder p = parameterPlaceholders.get(j);
            if (p instanceof OutParameterPlaceholder) {
                outs.add(new PlaceAndType(j + 1, p.type()));
                if (outs.size() == count) {
                    break;
                }
            }
        }
        st.execute();
        return outs;
    }

    private static final class PlaceAndType {
        final int pos;
        final Type type;

        PlaceAndType(int pos, Type type) {
            this.pos = pos;
            this.type = type;
        }

    }

    static PreparedStatement setParameters(PreparedStatement ps, List<Object> parameters,
            List<ParameterPlaceholder> parameterPlaceholders, List<String> names)
            throws SQLException {
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

}
