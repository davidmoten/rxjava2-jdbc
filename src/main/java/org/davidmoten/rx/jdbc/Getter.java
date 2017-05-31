package org.davidmoten.rx.jdbc;

import java.sql.ResultSet;

import org.davidmoten.rx.jdbc.tuple.Tuple2;
import org.davidmoten.rx.jdbc.tuple.Tuple3;
import org.davidmoten.rx.jdbc.tuple.Tuple4;
import org.davidmoten.rx.jdbc.tuple.Tuple5;
import org.davidmoten.rx.jdbc.tuple.Tuple6;
import org.davidmoten.rx.jdbc.tuple.Tuple7;
import org.davidmoten.rx.jdbc.tuple.TupleN;
import org.davidmoten.rx.jdbc.tuple.Tuples;

import io.reactivex.Flowable;
import io.reactivex.Single;

public interface Getter {

    /**
     * Transforms the results using the given function.
     *
     * @param function
     * @return the results of the query as an Observable
     */
    <T> Flowable<T> get(ResultSetMapper<? extends T> function);

    default <T> Flowable<T> getAs(Class<T> cls) {
        return get(rs -> Util.mapObject(rs, cls, 1));
    }

    /**
     * <p>
     * Transforms each row of the {@link ResultSet} into an instance of
     * <code>T</code> using <i>automapping</i> of the ResultSet columns into
     * corresponding constructor parameters that are assignable. Beyond normal
     * assignable criteria (for example Integer 123 is assignable to a Double)
     * other conversions exist to facilitate the automapping:
     * </p>
     * <p>
     * They are:
     * <ul>
     * <li>java.sql.Blob &#10143; byte[]</li>
     * <li>java.sql.Blob &#10143; java.io.InputStream</li>
     * <li>java.sql.Clob &#10143; String</li>s
     * <li>java.sql.Clob &#10143; java.io.Reader</li>
     * <li>java.sql.Date &#10143; java.util.Date</li>
     * <li>java.sql.Date &#10143; Long</li>
     * <li>java.sql.Timestamp &#10143; java.util.Date</li>
     * <li>java.sql.Timestamp &#10143; Long</li>
     * <li>java.sql.Time &#10143; java.util.Date</li>
     * <li>java.sql.Time &#10143; Long</li>
     * <li>java.math.BigInteger &#10143;
     * Short,Integer,Long,Float,Double,BigDecimal</li>
     * <li>java.math.BigDecimal &#10143;
     * Short,Integer,Long,Float,Double,BigInteger</li>
     * </p>
     *
     * @param cls
     *            class to automap each row of the ResultSet to
     * @param <T>
     *            generic type of returned stream emissions
     * @return Flowable of T
     * 
     */
    default <T> Flowable<T> autoMap(Class<T> cls) {
//        if (sql == null) {
//            // TODO get sql from query annotation
//            // sql = Util.getSqlFromQueryAnnotation(cls);
//        }
        return get(Util.autoMap(cls));
    }

    /**
     * Automaps all the columns of the {@link ResultSet} into the target class
     * <code>cls</code>. See {@link #autoMap(Class) autoMap()}.
     *
     * @param cls
     *            class of the TupleN elements
     * @param <T>
     *            generic type of returned stream emissions
     * @return a stream of TupleN
     */
    default <T> Flowable<TupleN<T>> getTupleN(Class<T> cls) {
        return get(Tuples.tupleN(cls));
    }

    /**
     * Automaps all the columns of the {@link ResultSet} into {@link Object} .
     * See {@link #autoMap(Class) autoMap()}.
     *
     * @param cls
     * @return
     */
    default <S> Flowable<TupleN<Object>> getTupleN() {
        return get(Tuples.tupleN(Object.class));
    }

    /**
     * Automaps the columns of the {@link ResultSet} into the specified classes.
     * See {@link #autoMap(Class) autoMap()}.
     *
     * @param cls1
     * @param cls2
     * @return
     */
    default <T1, T2> Flowable<Tuple2<T1, T2>> getAs(Class<T1> cls1, Class<T2> cls2) {
        return get(Tuples.tuple(cls1, cls2));
    }

    /**
     * Automaps the columns of the {@link ResultSet} into the specified classes.
     * See {@link #autoMap(Class) autoMap()}.
     *
     * @param cls1
     * @param cls2
     * @param cls3
     * @return
     */
    default <T1, T2, T3> Flowable<Tuple3<T1, T2, T3>> getAs(Class<T1> cls1, Class<T2> cls2, Class<T3> cls3) {
        return get(Tuples.tuple(cls1, cls2, cls3));
    }

    /**
     * Automaps the columns of the {@link ResultSet} into the specified classes.
     * See {@link #autoMap(Class) autoMap()}.
     *
     * @param cls1
     * @param cls2
     * @param cls3
     * @param cls4
     * @return
     */
    default <T1, T2, T3, T4> Flowable<Tuple4<T1, T2, T3, T4>> getAs(Class<T1> cls1, Class<T2> cls2, Class<T3> cls3,
            Class<T4> cls4) {
        return get(Tuples.tuple(cls1, cls2, cls3, cls4));
    }

    /**
     * Automaps the columns of the {@link ResultSet} into the specified classes.
     * See {@link #autoMap(Class) autoMap()}.
     *
     * @param cls1
     * @param cls2
     * @param cls3
     * @param cls4
     * @param cls5
     * @return
     */
    default <T1, T2, T3, T4, T5> Flowable<Tuple5<T1, T2, T3, T4, T5>> getAs(Class<T1> cls1, Class<T2> cls2,
            Class<T3> cls3, Class<T4> cls4, Class<T5> cls5) {
        return get(Tuples.tuple(cls1, cls2, cls3, cls4, cls5));
    }

    /**
     * Automaps the columns of the {@link ResultSet} into the specified classes.
     * See {@link #autoMap(Class) autoMap()}.
     *
     * @param cls1
     * @param cls2
     * @param cls3
     * @param cls4
     * @param cls5
     * @param cls6
     * @return
     */
    default <T1, T2, T3, T4, T5, T6> Flowable<Tuple6<T1, T2, T3, T4, T5, T6>> getAs(Class<T1> cls1, Class<T2> cls2,
            Class<T3> cls3, Class<T4> cls4, Class<T5> cls5, Class<T6> cls6) {
        return get(Tuples.tuple(cls1, cls2, cls3, cls4, cls5, cls6));
    }

    /**
     * Automaps the columns of the {@link ResultSet} into the specified classes.
     * See {@link #autoMap(Class) autoMap()}.
     *
     * @param cls1
     * @param cls2
     * @param cls3
     * @param cls4
     * @param cls5
     * @param cls6
     * @param cls7
     * @return
     */
    default <T1, T2, T3, T4, T5, T6, T7> Flowable<Tuple7<T1, T2, T3, T4, T5, T6, T7>> getAs(Class<T1> cls1,
            Class<T2> cls2, Class<T3> cls3, Class<T4> cls4, Class<T5> cls5, Class<T6> cls6, Class<T7> cls7) {
        return get(Tuples.tuple(cls1, cls2, cls3, cls4, cls5, cls6, cls7));
    }

    default Single<Long> count() {
        return get(rs -> 1).count();
    }
}
