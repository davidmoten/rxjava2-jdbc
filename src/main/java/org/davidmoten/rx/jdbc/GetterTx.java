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

public interface GetterTx {

    /**
     * Transforms the results using the given function.
     *
     * @param mapper
     *            transforms ResultSet row to an object of type T
     * @param <T>
     *            the type being mapped to
     * @return the results of the query as an Observable
     */
    <T> Flowable<Tx<T>> get(@Nonnull ResultSetMapper<? extends T> mapper);

    default <T> Flowable<Tx<T>> getAs(@Nonnull Class<T> cls) {
        Preconditions.checkNotNull(cls, "cls cannot be null");
        return get(rs -> Util.mapObject(rs, cls, 1));
    }

    default <T> Flowable<Tx<Optional<T>>> getAsOptional(@Nonnull Class<T> cls) {
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
    default <T> Flowable<Tx<T>> autoMap(@Nonnull Class<T> cls) {
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
     * @return stream of transaction items
     */
    default <T> Flowable<Tx<TupleN<T>>> getTupleN(@Nonnull Class<T> cls) {
        Preconditions.checkNotNull(cls, "cls cannot be null");
        return get(Tuples.tupleN(cls));
    }

    /**
     * Automaps all the columns of the {@link ResultSet} into {@link Object} . See
     * {@link #autoMap(Class) autoMap()}.
     *
     * @return stream of transaction items
     */
    default Flowable<Tx<TupleN<Object>>> getTupleN() {
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
     * @return stream of transaction items
     */
    default <T1, T2> Flowable<Tx<Tuple2<T1, T2>>> getAs(@Nonnull Class<T1> cls1,
            @Nonnull Class<T2> cls2) {
        Preconditions.checkNotNull(cls1, "cls1 cannot be null");
        Preconditions.checkNotNull(cls2, "cls2 cannot be null");
        return get(Tuples.tuple(cls1, cls2));
    }

    /**
     * Automaps the columns of the {@link ResultSet} into the specified classes. See
     * {@link #autoMap(Class) autoMap()}.
     *
     * @param cls1
     * @param cls2
     * @param cls3
     * @return stream of tuples
     */
    default <T1, T2, T3> Flowable<Tx<Tuple3<T1, T2, T3>>> getAs(@Nonnull Class<T1> cls1,
            @Nonnull Class<T2> cls2, @Nonnull Class<T3> cls3) {
        Preconditions.checkNotNull(cls1, "cls1 cannot be null");
        Preconditions.checkNotNull(cls2, "cls2 cannot be null");
        Preconditions.checkNotNull(cls3, "cls3 cannot be null");
        return get(Tuples.tuple(cls1, cls2, cls3));
    }

    /**
     * Automaps the columns of the {@link ResultSet} into the specified classes. See
     * {@link #autoMap(Class) autoMap()}.
     *
     * @param cls1
     * @param cls2
     * @param cls3
     * @param cls4
     * @return stream of tuples
     */
    default <T1, T2, T3, T4> Flowable<Tx<Tuple4<T1, T2, T3, T4>>> getAs(@Nonnull Class<T1> cls1,
            @Nonnull Class<T2> cls2, @Nonnull Class<T3> cls3, @Nonnull Class<T4> cls4) {
        Preconditions.checkNotNull(cls1, "cls1 cannot be null");
        Preconditions.checkNotNull(cls2, "cls2 cannot be null");
        Preconditions.checkNotNull(cls3, "cls3 cannot be null");
        Preconditions.checkNotNull(cls4, "cls4 cannot be null");
        return get(Tuples.tuple(cls1, cls2, cls3, cls4));
    }

    /**
     * Automaps the columns of the {@link ResultSet} into the specified classes. See
     * {@link #autoMap(Class) autoMap()}.
     *
     * @param cls1
     * @param cls2
     * @param cls3
     * @param cls4
     * @param cls5
     * @return stream of transaction items
     */
    default <T1, T2, T3, T4, T5> Flowable<Tx<Tuple5<T1, T2, T3, T4, T5>>> getAs(
            @Nonnull Class<T1> cls1, @Nonnull Class<T2> cls2, @Nonnull Class<T3> cls3,
            @Nonnull Class<T4> cls4, @Nonnull Class<T5> cls5) {
        Preconditions.checkNotNull(cls1, "cls1 cannot be null");
        Preconditions.checkNotNull(cls2, "cls2 cannot be null");
        Preconditions.checkNotNull(cls3, "cls3 cannot be null");
        Preconditions.checkNotNull(cls4, "cls4 cannot be null");
        Preconditions.checkNotNull(cls5, "cls5 cannot be null");
        return get(Tuples.tuple(cls1, cls2, cls3, cls4, cls5));
    }

    /**
     * Automaps the columns of the {@link ResultSet} into the specified classes. See
     * {@link #autoMap(Class) autoMap()}.
     *
     * @param cls1
     * @param cls2
     * @param cls3
     * @param cls4
     * @param cls5
     * @param cls6
     * @return stream of transaction items
     */
    default <T1, T2, T3, T4, T5, T6> Flowable<Tx<Tuple6<T1, T2, T3, T4, T5, T6>>> getAs(
            @Nonnull Class<T1> cls1, @Nonnull Class<T2> cls2, @Nonnull Class<T3> cls3,
            @Nonnull Class<T4> cls4, @Nonnull Class<T5> cls5, @Nonnull Class<T6> cls6) {
        Preconditions.checkNotNull(cls1, "cls1 cannot be null");
        Preconditions.checkNotNull(cls2, "cls2 cannot be null");
        Preconditions.checkNotNull(cls3, "cls3 cannot be null");
        Preconditions.checkNotNull(cls4, "cls4 cannot be null");
        Preconditions.checkNotNull(cls5, "cls5 cannot be null");
        Preconditions.checkNotNull(cls6, "cls6 cannot be null");
        return get(Tuples.tuple(cls1, cls2, cls3, cls4, cls5, cls6));
    }

    /**
     * Automaps the columns of the {@link ResultSet} into the specified classes. See
     * {@link #autoMap(Class) autoMap()}.
     *
     * @param cls1
     * @param cls2
     * @param cls3
     * @param cls4
     * @param cls5
     * @param cls6
     * @param cls7
     * @return stream of transaction items
     */
    default <T1, T2, T3, T4, T5, T6, T7> Flowable<Tx<Tuple7<T1, T2, T3, T4, T5, T6, T7>>> getAs(
            @Nonnull Class<T1> cls1, @Nonnull Class<T2> cls2, @Nonnull Class<T3> cls3,
            @Nonnull Class<T4> cls4, @Nonnull Class<T5> cls5, @Nonnull Class<T6> cls6,
            @Nonnull Class<T7> cls7) {
        Preconditions.checkNotNull(cls1, "cls1 cannot be null");
        Preconditions.checkNotNull(cls2, "cls2 cannot be null");
        Preconditions.checkNotNull(cls3, "cls3 cannot be null");
        Preconditions.checkNotNull(cls4, "cls4 cannot be null");
        Preconditions.checkNotNull(cls5, "cls5 cannot be null");
        Preconditions.checkNotNull(cls6, "cls6 cannot be null");
        Preconditions.checkNotNull(cls7, "cls7 cannot be null");
        return get(Tuples.tuple(cls1, cls2, cls3, cls4, cls5, cls6, cls7));
    }

    default Single<Long> count() {
        return get(rs -> 1).count();
    }
}
