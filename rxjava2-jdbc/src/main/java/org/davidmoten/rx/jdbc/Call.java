package org.davidmoten.rx.jdbc;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.Callable;

import org.davidmoten.rx.jdbc.CallableBuilder.In;
import org.davidmoten.rx.jdbc.CallableBuilder.InOut;
import org.davidmoten.rx.jdbc.CallableBuilder.OutParameterPlaceholder;
import org.davidmoten.rx.jdbc.CallableBuilder.ParameterPlaceholder;
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

    public static <T1> Flowable<Notification<T1>> create(Connection con, String sql,
            Flowable<List<Object>> parameterGroups,
            List<ParameterPlaceholder> parameterPlaceholders, Class<T1> cls) {
        log.debug("Update.create {}", sql);
        Callable<NamedCallableStatement> resourceFactory = () -> Util.prepareCall(con, sql,
                parameterPlaceholders);
        final Function<NamedCallableStatement, Flowable<Notification<T1>>> flowableFactory = //
                stmt -> parameterGroups //
                        .flatMap(parameters -> create(stmt, parameters, parameterPlaceholders, cls)
                                .toFlowable()) //
                        .materialize() //
                        .doOnComplete(() -> Util.commit(stmt.stmt)) //
                        .doOnError(e -> Util.rollback(stmt.stmt));
        Consumer<NamedCallableStatement> disposer = Util::closeCallableStatementAndConnection;
        return Flowable.using(resourceFactory, flowableFactory, disposer, true);
    }

    private static <T> Single<T> create(NamedCallableStatement stmt, List<Object> parameters,
            List<ParameterPlaceholder> parameterPlaceholders, Class<T> cls) {
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

    public static <T1> Flowable<Notification<T1>> create(Single<Connection> connection, String sql,
            Flowable<List<Object>> parameterGroups,
            List<ParameterPlaceholder> parameterPlaceholders, Class<T1> cls) {
        return connection.toFlowable()
                .flatMap(con -> create(con, sql, parameterGroups, parameterPlaceholders, cls));
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
