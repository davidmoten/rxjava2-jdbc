package org.davidmoten.rx.jdbc.tuple;

import static org.davidmoten.rx.jdbc.Util.mapObject;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.davidmoten.rx.jdbc.ResultSetMapper;
import org.davidmoten.rx.jdbc.exceptions.SQLRuntimeException;

/**
 * Utility methods for tuples.
 */
public final class Tuples {

    /**
     * Private constructor to prevent instantiation.
     */
    private Tuples() {
        // prevent instantiation.
    }

    public static <T> ResultSetMapper<T> single(final Class<T> cls) {
        return new ResultSetMapper<T>() {

            @Override
            public T apply(ResultSet rs) {
                return mapObject(rs, cls, 1);
            }

        };
    }

    public static <T1, T2> ResultSetMapper<Tuple2<T1, T2>> tuple(final Class<T1> cls1,
            final Class<T2> cls2) {
        return new ResultSetMapper<Tuple2<T1, T2>>() {

            @Override
            public Tuple2<T1, T2> apply(ResultSet rs) {
                return new Tuple2<T1, T2>(mapObject(rs, cls1, 1), mapObject(rs, cls2, 2));
            }
        };
    }

    public static <T1, T2, T3> ResultSetMapper<Tuple3<T1, T2, T3>> tuple(final Class<T1> cls1,
            final Class<T2> cls2, final Class<T3> cls3) {
        return new ResultSetMapper<Tuple3<T1, T2, T3>>() {
            @Override
            public Tuple3<T1, T2, T3> apply(ResultSet rs) {
                return new Tuple3<T1, T2, T3>(mapObject(rs, cls1, 1),
                        mapObject(rs, cls2, 2), mapObject(rs, cls3, 3));
            }
        };
    }

    public static <T1, T2, T3, T4> ResultSetMapper<Tuple4<T1, T2, T3, T4>> tuple(
            final Class<T1> cls1, final Class<T2> cls2, final Class<T3> cls3,
            final Class<T4> cls4) {
        return new ResultSetMapper<Tuple4<T1, T2, T3, T4>>() {
            @Override
            public Tuple4<T1, T2, T3, T4> apply(ResultSet rs) {
                return new Tuple4<T1, T2, T3, T4>(mapObject(rs, cls1, 1),
                        mapObject(rs, cls2, 2), mapObject(rs, cls3, 3),
                        mapObject(rs, cls4, 4));
            }
        };
    }

    public static <T1, T2, T3, T4, T5> ResultSetMapper<Tuple5<T1, T2, T3, T4, T5>> tuple(
            final Class<T1> cls1, final Class<T2> cls2, final Class<T3> cls3, final Class<T4> cls4,
            final Class<T5> cls5) {
        return new ResultSetMapper<Tuple5<T1, T2, T3, T4, T5>>() {
            @Override
            public Tuple5<T1, T2, T3, T4, T5> apply(ResultSet rs) {
                return new Tuple5<T1, T2, T3, T4, T5>(mapObject(rs, cls1, 1),
                        mapObject(rs, cls2, 2), mapObject(rs, cls3, 3),
                        mapObject(rs, cls4, 4), mapObject(rs, cls5, 5));
            }
        };
    }

    public static <T1, T2, T3, T4, T5, T6> ResultSetMapper<Tuple6<T1, T2, T3, T4, T5, T6>> tuple(
            final Class<T1> cls1, final Class<T2> cls2, final Class<T3> cls3, final Class<T4> cls4,
            final Class<T5> cls5, final Class<T6> cls6) {

        return new ResultSetMapper<Tuple6<T1, T2, T3, T4, T5, T6>>() {
            @Override
            public Tuple6<T1, T2, T3, T4, T5, T6> apply(ResultSet rs) {
                return new Tuple6<T1, T2, T3, T4, T5, T6>(mapObject(rs, cls1, 1),
                        mapObject(rs, cls2, 2), mapObject(rs, cls3, 3),
                        mapObject(rs, cls4, 4), mapObject(rs, cls5, 5),
                        mapObject(rs, cls6, 6));
            }
        };
    }

    public static <T1, T2, T3, T4, T5, T6, T7> ResultSetMapper<Tuple7<T1, T2, T3, T4, T5, T6, T7>> tuple(
            final Class<T1> cls1, final Class<T2> cls2, final Class<T3> cls3, final Class<T4> cls4,
            final Class<T5> cls5, final Class<T6> cls6, final Class<T7> cls7) {

        return new ResultSetMapper<Tuple7<T1, T2, T3, T4, T5, T6, T7>>() {
            @Override
            public Tuple7<T1, T2, T3, T4, T5, T6, T7> apply(ResultSet rs) {
                return new Tuple7<T1, T2, T3, T4, T5, T6, T7>(mapObject(rs, cls1, 1),
                        mapObject(rs, cls2, 2), mapObject(rs, cls3, 3),
                        mapObject(rs, cls4, 4), mapObject(rs, cls5, 5),
                        mapObject(rs, cls6, 6), mapObject(rs, cls7, 7));
            }
        };
    }

    public static <T> ResultSetMapper<TupleN<T>> tupleN(final Class<T> cls) {
        return new ResultSetMapper<TupleN<T>>() {
            @Override
            public TupleN<T> apply(ResultSet rs) {
                return toTupleN(cls, rs);
            }
        };
    }

    private static <T> TupleN<T> toTupleN(final Class<T> cls, ResultSet rs) {
        try {
            int n = rs.getMetaData().getColumnCount();
            List<T> list = new ArrayList<T>();
            for (int i = 1; i <= n; i++) {
                list.add(mapObject(rs, cls, i));
            }
            return new TupleN<T>(list);
        } catch (SQLException e) {
            throw new SQLRuntimeException(e);
        }
    }
}
