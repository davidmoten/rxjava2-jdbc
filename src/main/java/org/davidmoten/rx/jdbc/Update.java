package org.davidmoten.rx.jdbc;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.Callable;

import io.reactivex.Emitter;
import io.reactivex.Flowable;
import io.reactivex.Single;
import io.reactivex.functions.BiConsumer;
import io.reactivex.functions.Consumer;
import io.reactivex.functions.Function;

public enum Update {
	;
    
	public static Flowable<Integer> create(Flowable<Connection> connections, Flowable<List<Object>> parameterGroups,
			String sql) {
		return connections //
				.firstOrError() //
				.toFlowable() //
				.flatMap(con -> create(con, sql, parameterGroups), true, 1);
	}

	private static Flowable<Integer> create(Connection con, String sql, Flowable<List<Object>> parameterGroups) {
		Callable<NamedPreparedStatement> resourceFactory = () -> Util.prepare(con, sql);
		Function<NamedPreparedStatement, Flowable<Integer>> observableFactory = ps -> parameterGroups
				.flatMap(parameters -> create(ps, parameters).toFlowable());
		Consumer<NamedPreparedStatement> disposer = ps -> Util.closePreparedStatementAndConnection(ps.ps);
		return Flowable.using(resourceFactory, observableFactory, disposer, true);
	}

	private static Single<Integer> create(NamedPreparedStatement ps, List<Object> parameters) {
		return Single.fromCallable(() -> {
			Util.setParameters(ps.ps, parameters, ps.names);
			return ps.ps.executeUpdate();
		});
	}

	public static <T> Flowable<T> createReturnGeneratedKeys(Flowable<Connection> connections,
			Flowable<List<Object>> parameterGroups, String sql, Function<? super ResultSet, T> mapper) {
		return connections //
				.firstOrError() //
				.toFlowable() //
				.flatMap(con -> createReturnGeneratedKeys(con, parameterGroups, sql, mapper), true, 1);
	}

	private static <T> Flowable<T> createReturnGeneratedKeys(Connection con, Flowable<List<Object>> parameterGroups,
			String sql, Function<? super ResultSet, T> mapper) {
		Callable<NamedPreparedStatement> resourceFactory = () -> Util.prepareReturnGeneratedKeys(con, sql);
		Function<NamedPreparedStatement, Flowable<T>> obsFactory = ps -> parameterGroups
				.flatMap(parameters -> create(ps, parameters, mapper), true, 1);
		Consumer<NamedPreparedStatement> disposer = ps ->  Util.closePreparedStatementAndConnection(ps.ps);
		return Flowable.using(resourceFactory, obsFactory, disposer);
	}

	private static <T> Flowable<T> create(NamedPreparedStatement ps, List<Object> parameters,
			Function<? super ResultSet, T> mapper) {
		Callable<ResultSet> initialState = () -> {
			Util.setParameters(ps.ps, parameters, ps.names);
			ps.ps.execute();
			return ps.ps.getGeneratedKeys();
		};
		BiConsumer<ResultSet, Emitter<T>> generator = (rs, emitter) -> {
			if (rs.next()) {
				emitter.onNext(mapper.apply(rs));
			} else {
				emitter.onComplete();
			}
		};
		Consumer<ResultSet> disposer = rs -> {
			try {
				rs.close();
			} catch (SQLException e) {
			}
		};
		return Flowable.generate(initialState, generator, disposer);
	}

}
