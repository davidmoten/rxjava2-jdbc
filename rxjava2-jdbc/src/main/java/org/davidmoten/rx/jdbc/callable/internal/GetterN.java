package org.davidmoten.rx.jdbc.callable.internal;

import java.sql.ResultSet;

import javax.annotation.Nonnull;

import org.davidmoten.rx.jdbc.CallableBuilder.CallableResultSetsNBuilder;
import org.davidmoten.rx.jdbc.Util;
import org.davidmoten.rx.jdbc.tuple.Tuples;

import com.github.davidmoten.guavamini.Preconditions;

import io.reactivex.functions.Function;

public interface GetterN {

    CallableResultSetsNBuilder get(Function<? super ResultSet, ?> function);

    default <T> CallableResultSetsNBuilder getAs(@Nonnull Class<T> cls) {
        Preconditions.checkNotNull(cls, "cls cannot be null");
        return get(rs -> Util.mapObject(rs, cls, 1));
    }

    default <T1, T2> CallableResultSetsNBuilder getAs(@Nonnull Class<T1> cls1,
            @Nonnull Class<T2> cls2) {
        return get(Tuples.tuple(cls1, cls2));
    }

    default <T1, T2, T3> CallableResultSetsNBuilder getAs(@Nonnull Class<T1> cls1,
            @Nonnull Class<T2> cls2, @Nonnull Class<T3> cls3) {
        return get(Tuples.tuple(cls1, cls2, cls3));
    }

    default <T1, T2, T3, T4> CallableResultSetsNBuilder getAs(@Nonnull Class<T1> cls1,
            @Nonnull Class<T2> cls2, @Nonnull Class<T3> cls3, @Nonnull Class<T4> cls4) {
        return get(Tuples.tuple(cls1, cls2, cls3, cls4));
    }

    default <T1, T2, T3, T4, T5> CallableResultSetsNBuilder getAs(@Nonnull Class<T1> cls1,
            @Nonnull Class<T2> cls2, @Nonnull Class<T3> cls3, @Nonnull Class<T4> cls4, @Nonnull Class<T5> cls5) {
        return get(Tuples.tuple(cls1, cls2, cls3, cls4, cls5));
    }

    default <T1, T2, T3, T4, T5, T6> CallableResultSetsNBuilder getAs(
            @Nonnull Class<T1> cls1, @Nonnull Class<T2> cls2, @Nonnull Class<T3> cls3, @Nonnull Class<T4> cls4,
            @Nonnull Class<T5> cls5, @Nonnull Class<T6> cls6) {
        return get(Tuples.tuple(cls1, cls2, cls3, cls4, cls5, cls6));
    }

    default <T1, T2, T3, T4, T5, T6, T7> CallableResultSetsNBuilder getAs(
            @Nonnull Class<T1> cls1, @Nonnull Class<T2> cls2, @Nonnull Class<T3> cls3, @Nonnull Class<T4> cls4,
            @Nonnull Class<T5> cls5, @Nonnull Class<T6> cls6, @Nonnull Class<T7> cls7) {
        return get(Tuples.tuple(cls1, cls2, cls3, cls4, cls5, cls6, cls7));
    }
}
