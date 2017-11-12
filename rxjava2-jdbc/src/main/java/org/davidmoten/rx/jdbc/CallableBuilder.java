package org.davidmoten.rx.jdbc;

import java.sql.Connection;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.davidmoten.rx.jdbc.tuple.Tuple2;

import com.github.davidmoten.guavamini.Preconditions;

import io.reactivex.Completable;
import io.reactivex.Flowable;
import io.reactivex.Single;
import io.reactivex.functions.Function;

public final class CallableBuilder {

    final String sql;
    final List<ParameterPlaceholder> params = new ArrayList<>();

    Flowable<?> inStream;

    private final Single<Connection> connection;

    public CallableBuilder(String sql, Single<Connection> connection) {
        this.sql = sql;
        this.connection = connection;
    }

    interface ParameterPlaceholder {
        Type type();
    }

    interface OutParameterPlaceholder extends ParameterPlaceholder {
    }

    interface InParameterPlaceholder extends ParameterPlaceholder {
    }

    static final class In implements InParameterPlaceholder {
        final Type type;

        In(Type type) {
            this.type = type;
        }

        @Override
        public Type type() {
            return type;
        }
    }

    static final class InOut implements InParameterPlaceholder, OutParameterPlaceholder {
        final Type type;
        final Class<?> cls;

        InOut(Type type, Class<?> cls) {
            this.type = type;
            this.cls = cls;
        }

        @Override
        public Type type() {
            return type;
        }
    }

    static final class Out implements OutParameterPlaceholder {
        final Type type;
        final Class<?> cls;

        public Out(Type type, Class<?> cls) {
            this.type = type;
            this.cls = cls;
        }

        @Override
        public Type type() {
            return type;
        }
    }

    @SuppressWarnings("unchecked")
    public Flowable<List<Object>> parameterGroups() {
        int numInParameters = params.stream() //
                .filter(x -> x instanceof InParameterPlaceholder) //
                .collect(Collectors.counting()).intValue();
        return (Flowable<List<Object>>) (Flowable<?>) inStream.buffer(numInParameters);

    }

    public Completable perform() {
        // TODO
        return null;
    }

    public CallableBuilder in(Type type) {
        params.add(new In(type));
        return this;
    }

    public CallableBuilder in(Flowable<?> f) {
        Preconditions.checkArgument(inStream == null, "you can only specify in flowable once");
        this.inStream = f;
        return this;
    }

    public CallableBuilder in(Object... objects) {
        in(Flowable.fromArray(objects));
        return this;
    }

    public <T> CallableBuilder1<T> inOut(Type type, Class<T> cls) {
        params.add(new InOut(type, cls));
        return new CallableBuilder1<T>(this, cls);
    }

    public <T> CallableBuilder1<T> out(Type type, Class<T> cls) {
        params.add(new Out(type, cls));
        return new CallableBuilder1<T>(this, cls);
    }

    public <T> CallableResultSets1Builder<T> map(Function<? super ResultSet, T> function) {
        return new CallableResultSets1Builder<T>(this, function);
    }

    public <T> CallableResultSets1Builder<T> autoMap(Class<T> cls) {
        return map(Util.autoMap(cls));
    }

    public static final class CallableBuilder1<T1> {

        private final CallableBuilder b;
        private final Class<T1> cls;

        public CallableBuilder1(CallableBuilder b, Class<T1> cls) {
            this.b = b;
            this.cls = cls;
        }

        public CallableBuilder1<T1> in(Type type) {
            b.in(type);
            return this;
        }

        public <T2> CallableBuilder2<T1, T2> out(Type type, Class<T2> cls2) {
            b.out(type, cls2);
            return new CallableBuilder2<T1, T2>(b, cls, cls2);
        }

        public CallableBuilder1<T1> in(Flowable<?> f) {
            b.in(f);
            return this;
        }

        public CallableBuilder1<T1> in(Object... objects) {
            in(Flowable.fromArray(objects));
            return this;
        }

        public Flowable<T1> build() {
            return Call
                    .createWithOneOutParameter(b.connection, b.sql, b.parameterGroups(), b.params,
                            cls) //
                    .dematerialize();
        }
    }

    public static final class CallableBuilder2<T1, T2> {

        private final CallableBuilder b;
        private final Class<T1> cls1;
        private final Class<T2> cls2;

        public CallableBuilder2(CallableBuilder b, Class<T1> cls1, Class<T2> cls2) {
            this.b = b;
            this.cls1 = cls1;
            this.cls2 = cls2;
        }

        public CallableBuilder2<T1, T2> in(Flowable<?> f) {
            b.in(f);
            return this;
        }

        public CallableBuilder2<T1, T2> in(Object... objects) {
            in(Flowable.fromArray(objects));
            return this;
        }

        public Flowable<Tuple2<T1, T2>> build() {
            return Call
                    .createWithTwoOutParameters(b.connection, b.sql, b.parameterGroups(), b.params,
                            cls1, cls2) //
                    .dematerialize();
        }
    }

    public static final class CallableResultSets1Builder<T1> {

        private final CallableBuilder b;
        private final Function<? super ResultSet, ? extends T1> f1;

        CallableResultSets1Builder(CallableBuilder b,
                Function<? super ResultSet, ? extends T1> function) {
            this.b = b;
            this.f1 = function;
        }

        public <T2> CallableResultSets2Builder<T1, T2> autoMap(Class<T2> cls) {
            return map(Util.autoMap(cls));
        }

        public <T2> CallableResultSets2Builder<T1, T2> map(
                Function<? super ResultSet, ? extends T2> f2) {
            return new CallableResultSets2Builder<T1, T2>(b, f1, f2);
        }

        public CallableResultSets1Builder<T1> in(Flowable<?> f) {
            b.in(f);
            return this;
        }

        public CallableResultSets1Builder<T1> in(Object... objects) {
            return in(Flowable.fromArray(objects));
        }

        public Flowable<T1> build() {
            // TODO
            return null;
        }

    }

    public static final class CallableResultSets2Builder<T1, T2> {

        private final CallableBuilder b;
        private final Function<? super ResultSet, ? extends T1> f1;
        private final Function<? super ResultSet, ? extends T2> f2;

        CallableResultSets2Builder(CallableBuilder b, Function<? super ResultSet, ? extends T1> f1,
                Function<? super ResultSet, ? extends T2> f2) {
            this.b = b;
            this.f1 = f1;
            this.f2 = f2;
        }

        public Single<CallableResultSet2<T1, T2>> build() {
            // TODO Auto-generated method stub
            return null;
        }
    }

    public static final class CallableResultSet2<T1, T2> {

        public Flowable<T1> query1() {
            return null;
        }

        public Flowable<T2> query2() {
            return null;
        }

    }

    public static final class CallableResultSet1<T1> {

        public Flowable<T1> query1() {
            return null;
        }

    }

}
