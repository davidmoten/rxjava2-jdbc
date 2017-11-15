package org.davidmoten.rx.jdbc.callable.internal;

import java.sql.ResultSet;

import javax.annotation.Nonnull;

import org.davidmoten.rx.jdbc.CallableBuilder.CallableResultSets3Builder;
import org.davidmoten.rx.jdbc.Util;
import org.davidmoten.rx.jdbc.tuple.Tuple2;
import org.davidmoten.rx.jdbc.tuple.Tuple3;
import org.davidmoten.rx.jdbc.tuple.Tuple4;
import org.davidmoten.rx.jdbc.tuple.Tuple5;
import org.davidmoten.rx.jdbc.tuple.Tuple6;
import org.davidmoten.rx.jdbc.tuple.Tuple7;
import org.davidmoten.rx.jdbc.tuple.Tuples;

import com.github.davidmoten.guavamini.Preconditions;

import io.reactivex.functions.Function;

public interface Getter3<T1, T2> {

    <T3> CallableResultSets3Builder<T1, T2, T3> get(Function<? super ResultSet, ? extends T3> function);

    default <T3> CallableResultSets3Builder<T1, T2, T3> getAs(@Nonnull Class<T3> cls) {
        Preconditions.checkNotNull(cls, "cls cannot be null");
        return get(rs -> Util.mapObject(rs, cls, 1));
    }

    default <A, B> CallableResultSets3Builder<T1, T2, Tuple2<A, B>> getAs(@Nonnull Class<A> cls1,
            @Nonnull Class<B> cls2) {
        return get(Tuples.tuple(cls1, cls2));
    }

    default <A, B, C> CallableResultSets3Builder<T1, T2, Tuple3<A, B, C>> getAs(@Nonnull Class<A> cls1,
            @Nonnull Class<B> cls2, @Nonnull Class<C> cls3) {
        return get(Tuples.tuple(cls1, cls2, cls3));
    }

    default <A, B, C, D> CallableResultSets3Builder<T1, T2, Tuple4<A, B, C, D>> getAs(@Nonnull Class<A> cls1,
            @Nonnull Class<B> cls2, @Nonnull Class<C> cls3, @Nonnull Class<D> cls4) {
        return get(Tuples.tuple(cls1, cls2, cls3, cls4));
    }

    default <A, B, C, D, E> CallableResultSets3Builder<T1, T2, Tuple5<A, B, C, D, E>> getAs(@Nonnull Class<A> cls1,
            @Nonnull Class<B> cls2, @Nonnull Class<C> cls3, @Nonnull Class<D> cls4, @Nonnull Class<E> cls5) {
        return get(Tuples.tuple(cls1, cls2, cls3, cls4, cls5));
    }

    default <A, B, C, D, E, F> CallableResultSets3Builder<T1, T2, Tuple6<A, B, C, D, E, F>> getAs(
            @Nonnull Class<A> cls1, @Nonnull Class<B> cls2, @Nonnull Class<C> cls3, @Nonnull Class<D> cls4,
            @Nonnull Class<E> cls5, @Nonnull Class<F> cls6) {
        return get(Tuples.tuple(cls1, cls2, cls3, cls4, cls5, cls6));
    }

    default <A, B, C, D, E, F, G> CallableResultSets3Builder<T1, T2, Tuple7<A, B, C, D, E, F, G>> getAs(
            @Nonnull Class<A> cls1, @Nonnull Class<B> cls2, @Nonnull Class<C> cls3, @Nonnull Class<D> cls4,
            @Nonnull Class<E> cls5, @Nonnull Class<F> cls6, @Nonnull Class<G> cls7) {
        return get(Tuples.tuple(cls1, cls2, cls3, cls4, cls5, cls6, cls7));
    }

}
