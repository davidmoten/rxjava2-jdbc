package org.davidmoten.rx.jdbc;

import java.sql.ResultSet;
import java.util.Optional;

import javax.annotation.Nonnull;

import org.davidmoten.rx.jdbc.tuple.Tuple2;
import org.davidmoten.rx.jdbc.tuple.Tuple3;
import org.davidmoten.rx.jdbc.tuple.Tuple4;
import org.davidmoten.rx.jdbc.tuple.Tuple5;
import org.davidmoten.rx.jdbc.tuple.Tuple6;
import org.davidmoten.rx.jdbc.tuple.Tuple7;
import org.davidmoten.rx.jdbc.tuple.TupleN;
import org.davidmoten.rx.jdbc.tuple.Tuples;

import com.github.davidmoten.guavamini.Preconditions;

import io.reactivex.Flowable;
import io.reactivex.Single;

public interface Getter {

    /**
     * Transforms the results using the given function.
     *
     * @param mapper
     *            transforms ResultSet rows to an object of type T
     * @param <T>
     *            the type being mapped to
     * @return the results of the query as an Observable
     */
    <T> Flowable<T> get(@Nonnull ResultSetMapper<? extends T> mapper);

    default <T> Flowable<T> getAs(@Nonnull Class<T> cls) {
        Preconditions.checkNotNull(cls, "cls cannot be null");
        return get(rs -> Util.mapObject(rs, cls, 1));
    }

    default <T> Flowable<Optional<T>> getAsOptional(@Nonnull Class<T> cls) {
        Preconditions.checkNotNull(cls, "cls cannot be null");
        return get(rs -> Optional.ofNullable(Util.mapObject(rs, cls, 1)));
    }

    /**
     * <p>
     * Transforms each row of the {@link ResultSet} into an instance of
     * <code>T</code> using <i>automapping</i> of the ResultSet columns into
     * corresponding constructor parameters that are assignable. Beyond normal
     * assignable criteria (for example Integer 123 is assignable to a Double) other
     * conversions exist to facilitate the automapping:
     * </p>
     * <p>
     * They are:
     * <ul>
     * <li>java.sql.Blob --&gt; byte[]</li>
     * <li>java.sql.Blob --&gt; java.io.InputStream</li>
     * <li>java.sql.Clob --&gt; String</li>
     * <li>java.sql.Clob --&gt; java.io.Reader</li>
     * <li>java.sql.Date --&gt; java.util.Date</li>
     * <li>java.sql.Date --&gt; Long</li>
     * <li>java.sql.Timestamp --&gt; java.util.Date</li>
     * <li>java.sql.Timestamp --&gt; Long</li>
     * <li>java.sql.Time --&gt; java.util.Date</li>
     * <li>java.sql.Time --&gt; Long</li>
     * <li>java.math.BigInteger --&gt;
     * Short,Integer,Long,Float,Double,BigDecimal</li>
     * <li>java.math.BigDecimal --&gt;
     * Short,Integer,Long,Float,Double,BigInteger</li>
     * </ul>
     *
     * @param cls
     *            class to automap each row of the ResultSet to
     * @param <T>
     *            generic type of returned stream emissions
     * @return Flowable of T
     * 
     */
    default <T> Flowable<T> autoMap(@Nonnull Class<T> cls) {
        Preconditions.checkNotNull(cls, "cls cannot be null");
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
    default <T> Flowable<TupleN<T>> getTupleN(@Nonnull Class<T> cls) {
        Preconditions.checkNotNull(cls, "cls cannot be null");
        return get(Tuples.tupleN(cls));
    }

    /**
     * Automaps all the columns of the {@link ResultSet} into {@link Object} . See
     * {@link #autoMap(Class) autoMap()}.
     *
     * @return stream of TupleN
     */
    default Flowable<TupleN<Object>> getTupleN() {
        return get(Tuples.tupleN(Object.class));
    }

    /**
     * Automaps the columns of the {@link ResultSet} into the specified classes. See
     * {@link #autoMap(Class) autoMap()}.
     *
     * @param cls1
     *            first class
     * @param cls2
     *            second class
     * @param <T1>
     *            type of first class
     * @param <T2>
     *            type of second class
     * @return flowable
     */
    default <T1, T2> Flowable<Tuple2<T1, T2>> getAs(@Nonnull Class<T1> cls1,
            @Nonnull Class<T2> cls2) {
        return get(Tuples.tuple(cls1, cls2));
    }

    /**
     * Automaps the columns of the {@link ResultSet} into the specified classes. See
     * {@link #autoMap(Class) autoMap()}.
     *
     * @param cls1
     *            first class
     * @param cls2
     *            second class
     * @param cls3
     *            third class
     * @param <T1>
     *            type of first class
     * @param <T2>
     *            type of second class
     * @param <T3>
     *            type of third class
     * @return tuple
     */
    default <T1, T2, T3> Flowable<Tuple3<T1, T2, T3>> getAs(@Nonnull Class<T1> cls1,
            @Nonnull Class<T2> cls2, @Nonnull Class<T3> cls3) {
        return get(Tuples.tuple(cls1, cls2, cls3));
    }

    /**
     * Automaps the columns of the {@link ResultSet} into the specified classes. See
     * {@link #autoMap(Class) autoMap()}.
     *
     * @param cls1
     *            first class
     * @param cls2
     *            second class
     * @param cls3
     *            third class
     * @param cls4
     *            fourth class
     * @param <T1>
     *            type of first class
     * @param <T2>
     *            type of second class
     * @param <T3>
     *            type of third class
     * @param <T4>
     *            type of fourth class
     * @return tuple
     */
    default <T1, T2, T3, T4> Flowable<Tuple4<T1, T2, T3, T4>> getAs(@Nonnull Class<T1> cls1,
            @Nonnull Class<T2> cls2, @Nonnull Class<T3> cls3, @Nonnull Class<T4> cls4) {
        return get(Tuples.tuple(cls1, cls2, cls3, cls4));
    }

    /**
     * Automaps the columns of the {@link ResultSet} into the specified classes. See
     * {@link #autoMap(Class) autoMap()}.
     *
     * @param cls1
     *            first class
     * @param cls2
     *            second class
     * @param cls3
     *            third class
     * @param cls4
     *            fourth class
     * @param cls5
     *            fifth class
     * @param <T1>
     *            type of first class
     * @param <T2>
     *            type of second class
     * @param <T3>
     *            type of third class
     * @param <T4>
     *            type of fourth class
     * @param <T5>
     *            type of fifth class
     * @return tuple
     */
    default <T1, T2, T3, T4, T5> Flowable<Tuple5<T1, T2, T3, T4, T5>> getAs(@Nonnull Class<T1> cls1,
            @Nonnull Class<T2> cls2, @Nonnull Class<T3> cls3, @Nonnull Class<T4> cls4,
            @Nonnull Class<T5> cls5) {
        return get(Tuples.tuple(cls1, cls2, cls3, cls4, cls5));
    }

    /**
     * Automaps the columns of the {@link ResultSet} into the specified classes. See
     * {@link #autoMap(Class) autoMap()}.
     *
     * @param cls1
     *            first class
     * @param cls2
     *            second class
     * @param cls3
     *            third class
     * @param cls4
     *            fourth class
     * @param cls5
     *            fifth class
     * @param cls6
     *            sixth class
     * @param <T1>
     *            type of first class
     * @param <T2>
     *            type of second class
     * @param <T3>
     *            type of third class
     * @param <T4>
     *            type of fourth class
     * @param <T5>
     *            type of fifth class
     * @param <T6>
     *            type of sixth class
     * @return tuple
     */
    default <T1, T2, T3, T4, T5, T6> Flowable<Tuple6<T1, T2, T3, T4, T5, T6>> getAs(
            @Nonnull Class<T1> cls1, @Nonnull Class<T2> cls2, @Nonnull Class<T3> cls3,
            @Nonnull Class<T4> cls4, @Nonnull Class<T5> cls5, @Nonnull Class<T6> cls6) {
        return get(Tuples.tuple(cls1, cls2, cls3, cls4, cls5, cls6));
    }

    /**
     * Automaps the columns of the {@link ResultSet} into the specified classes. See
     * {@link #autoMap(Class) autoMap()}.
     *
     * @param cls1
     *            first class
     * @param cls2
     *            second class
     * @param cls3
     *            third class
     * @param cls4
     *            fourth class
     * @param cls5
     *            fifth class
     * @param cls6
     *            sixth class
     * @param cls7
     *            seventh class
     * @param <T1>
     *            type of first class
     * @param <T2>
     *            type of second class
     * @param <T3>
     *            type of third class
     * @param <T4>
     *            type of fourth class
     * @param <T5>
     *            type of fifth class
     * @param <T6>
     *            type of sixth class
     * @param <T7>
     *            type of seventh class
     * @return tuple
     */
    default <T1, T2, T3, T4, T5, T6, T7> Flowable<Tuple7<T1, T2, T3, T4, T5, T6, T7>> getAs(
            @Nonnull Class<T1> cls1, @Nonnull Class<T2> cls2, @Nonnull Class<T3> cls3,
            @Nonnull Class<T4> cls4, @Nonnull Class<T5> cls5, @Nonnull Class<T6> cls6,
            @Nonnull Class<T7> cls7) {
        return get(Tuples.tuple(cls1, cls2, cls3, cls4, cls5, cls6, cls7));
    }

    default Single<Long> count() {
        return get(rs -> 1).count();
    }
}
