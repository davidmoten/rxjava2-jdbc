package org.davidmoten.rx.jdbc;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.Callable;

import io.reactivex.Emitter;
import io.reactivex.Flowable;
import io.reactivex.functions.BiConsumer;
import io.reactivex.functions.Consumer;
import io.reactivex.functions.Function;

public class FlowableSelect {
	
	public static <T> Flowable<T> create(Callable<Connection> connectionFactory, List<Object> parameters, String sql,
			Function<? super ResultSet, T> mapper) {
		Callable<Query> initialState = () -> {
			Connection con = connectionFactory.call();
			PreparedStatement ps = con.prepareStatement(sql);
			ResultSet rs = ps.executeQuery();
			return new Query(ps, rs);
		};
		BiConsumer<Query, Emitter<T>> generator = (query, emitter) -> {
			emitter.onNext(mapper.apply(query.rs));
		};
		Consumer<Query> disposeState = rs -> {
			rs.close();
		};
		return Flowable.generate(initialState, generator, disposeState);
	}

	private static class Query {
		final PreparedStatement ps;
		final ResultSet rs;

		Query(PreparedStatement ps, ResultSet rs) {
			this.ps = ps;
			this.rs = rs;
		}
		
		void close() {
			try {
				rs.close();
			} catch (SQLException e) {
				//ignore
			}
			Connection con = null;
			try {
				con  = ps.getConnection();
			} catch (SQLException e1) {
				// ignore
			}
			try {
				ps.close();
			} catch (SQLException e) {
				// ignore
			}
			if (con != null) {
				try {
					con.close();
				} catch (SQLException e) {
					// ignore
				}
			}
		}
	}

}
