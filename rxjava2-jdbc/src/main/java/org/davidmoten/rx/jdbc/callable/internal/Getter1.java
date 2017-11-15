package org.davidmoten.rx.jdbc.callable.internal;

import java.sql.ResultSet;

import javax.annotation.Nonnull;

import org.davidmoten.rx.jdbc.CallableBuilder.CallableResultSets1Builder;
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

public interface Getter1 {

    <T> CallableResultSets1Builder<T> get(Function<? super ResultSet, ? extends T> function);

    default <T> CallableResultSets1Builder<T> getAs(@Nonnull Class<T> cls) {
        Preconditions.checkNotNull(cls, "cls cannot be null");
        return get(rs -> Util.mapObject(rs, cls, 1));
    }

    default <T1, T2> CallableResultSets1Builder<Tuple2<T1, T2>> getAs(@Nonnull Class<T1> cls1,
            @Nonnull Class<T2> cls2) {
        Preconditions.checkNotNull(cls1, "cls1 cannot be null");
        Preconditions.checkNotNull(cls2, "cls2 cannot be null");
        return get(Tuples.tuple(cls1, cls2));
    }

    default <T1, T2, T3> CallableResultSets1Builder<Tuple3<T1, T2, T3>> getAs(@Nonnull Class<T1> cls1,
            @Nonnull Class<T2> cls2, @Nonnull Class<T3> cls3) {
        Preconditions.checkNotNull(cls1, "cls1 cannot be null");
        Preconditions.checkNotNull(cls2, "cls2 cannot be null");
        Preconditions.checkNotNull(cls3, "cls3 cannot be null");
        return get(Tuples.tuple(cls1, cls2, cls3));
    }

    default <T1, T2, T3, T4> CallableResultSets1Builder<Tuple4<T1, T2, T3, T4>> getAs(@Nonnull Class<T1> cls1,
            @Nonnull Class<T2> cls2, @Nonnull Class<T3> cls3, @Nonnull Class<T4> cls4) {
        Preconditions.checkNotNull(cls1, "cls1 cannot be null");
        Preconditions.checkNotNull(cls2, "cls2 cannot be null");
        Preconditions.checkNotNull(cls3, "cls3 cannot be null");
        Preconditions.checkNotNull(cls4, "cls4 cannot be null");
        return get(Tuples.tuple(cls1, cls2, cls3, cls4));
    }

    default <T1, T2, T3, T4, T5> CallableResultSets1Builder<Tuple5<T1, T2, T3, T4, T5>> getAs(@Nonnull Class<T1> cls1,
            @Nonnull Class<T2> cls2, @Nonnull Class<T3> cls3, @Nonnull Class<T4> cls4, @Nonnull Class<T5> cls5) {
        Preconditions.checkNotNull(cls1, "cls1 cannot be null");
        Preconditions.checkNotNull(cls2, "cls2 cannot be null");
        Preconditions.checkNotNull(cls3, "cls3 cannot be null");
        Preconditions.checkNotNull(cls4, "cls4 cannot be null");
        Preconditions.checkNotNull(cls5, "cls5 cannot be null");
        return get(Tuples.tuple(cls1, cls2, cls3, cls4, cls5));
    }

    default <T1, T2, T3, T4, T5, T6> CallableResultSets1Builder<Tuple6<T1, T2, T3, T4, T5, T6>> getAs(
            @Nonnull Class<T1> cls1, @Nonnull Class<T2> cls2, @Nonnull Class<T3> cls3, @Nonnull Class<T4> cls4,
            @Nonnull Class<T5> cls5, @Nonnull Class<T6> cls6) {
        Preconditions.checkNotNull(cls1, "cls1 cannot be null");
        Preconditions.checkNotNull(cls2, "cls2 cannot be null");
        Preconditions.checkNotNull(cls3, "cls3 cannot be null");
        Preconditions.checkNotNull(cls4, "cls4 cannot be null");
        Preconditions.checkNotNull(cls5, "cls5 cannot be null");
        Preconditions.checkNotNull(cls6, "cls6 cannot be null");
        return get(Tuples.tuple(cls1, cls2, cls3, cls4, cls5, cls6));
    }

    default <T1, T2, T3, T4, T5, T6, T7> CallableResultSets1Builder<Tuple7<T1, T2, T3, T4, T5, T6, T7>> getAs(
            @Nonnull Class<T1> cls1, @Nonnull Class<T2> cls2, @Nonnull Class<T3> cls3, @Nonnull Class<T4> cls4,
            @Nonnull Class<T5> cls5, @Nonnull Class<T6> cls6, @Nonnull Class<T7> cls7) {
        Preconditions.checkNotNull(cls1, "cls1 cannot be null");
        Preconditions.checkNotNull(cls2, "cls2 cannot be null");
        Preconditions.checkNotNull(cls3, "cls3 cannot be null");
        Preconditions.checkNotNull(cls4, "cls4 cannot be null");
        Preconditions.checkNotNull(cls5, "cls5 cannot be null");
        Preconditions.checkNotNull(cls6, "cls6 cannot be null");
        Preconditions.checkNotNull(cls7, "cls7 cannot be null");
        return get(Tuples.tuple(cls1, cls2, cls3, cls4, cls5, cls6, cls7));
    }

}
