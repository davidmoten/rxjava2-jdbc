package org.davidmoten.rx.jdbc;

import java.sql.Connection;
import java.util.List;
import java.util.concurrent.Callable;

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
            Flowable<List<Object>> parameterGroups, Class<T1> cls) {
        log.debug("Update.create {}", sql);
        Callable<NamedCallableStatement> resourceFactory = () -> Util.prepareCall(con, sql);
        final Function<NamedCallableStatement, Flowable<Notification<T1>>> flowableFactory = //
                stmt -> parameterGroups //
                        .flatMap(parameters -> create(stmt, parameters, cls).toFlowable()) //
                        .materialize() //
                        .doOnComplete(() -> Util.commit(stmt.stmt)) //
                        .doOnError(e -> Util.rollback(stmt.stmt));
        Consumer<NamedCallableStatement> disposer = Util::closeCallableStatementAndConnection;
        return Flowable.using(resourceFactory, flowableFactory, disposer, true);
    }

    private static <T> Single<T> create(NamedCallableStatement stmt, List<Object> parameters,
            Class<T> cls) {
        return Single.fromCallable(() -> {
            Util.incrementCounter(stmt.stmt.getConnection());
            Util.setParameters(stmt.stmt, parameters, stmt.names);
            return Util.mapObject(stmt.stmt, cls, 1);
        });
    }

    public static <T1> Flowable<Notification<T1>> create(Single<Connection> connection, String sql,
            Flowable<List<Object>> parameterGroups, Class<T1> cls) {
        return connection.toFlowable().flatMap(con -> create(con, sql, parameterGroups, cls));
    }

}
