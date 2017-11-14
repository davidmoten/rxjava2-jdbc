package org.davidmoten.rx.jdbc;

import java.sql.Connection;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import org.davidmoten.rx.jdbc.tuple.Tuple2;
import org.davidmoten.rx.jdbc.tuple.Tuple3;
import org.davidmoten.rx.jdbc.tuple.Tuple4;

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
        if (numInParameters == 0) {
            return inStream.map(x -> Collections.singletonList(x));
        } else {
            return (Flowable<List<Object>>) (Flowable<?>) inStream.buffer(numInParameters);
        }
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
        Preconditions.checkArgument(inStream == null, "you can only specify in flowable once, current=" + inStream);
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
        
        public <T> CallableResultSets1Builder<T> map(Function<? super ResultSet, T> function) {
            return new CallableResultSets1Builder<T>(b, function);
        }

        public <T> CallableResultSets1Builder<T> autoMap(Class<T> cls) {
            return map(Util.autoMap(cls));
        }

        public Flowable<T1> build() {
            return Call.createWithOneOutParameter(b.connection, b.sql, b.parameterGroups(), b.params, cls) //
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

        public <T3> CallableBuilder3<T1, T2, T3> out(Type type, Class<T3> cls3) {
            b.out(type, cls3);
            return new CallableBuilder3<T1, T2, T3>(b, cls1, cls2, cls3);
        }

        public CallableBuilder2<T1, T2> in(Flowable<?> f) {
            b.in(f);
            return this;
        }

        public CallableBuilder2<T1, T2> in(Object... objects) {
            in(Flowable.fromArray(objects));
            return this;
        }
        
        public <T> CallableResultSets1Builder<T> map(Function<? super ResultSet, T> function) {
            return new CallableResultSets1Builder<T>(b, function);
        }

        public <T> CallableResultSets1Builder<T> autoMap(Class<T> cls) {
            return map(Util.autoMap(cls));
        }

        public Flowable<Tuple2<T1, T2>> build() {
            return Call.createWithTwoOutParameters(b.connection, b.sql, b.parameterGroups(), b.params, cls1, cls2) //
                    .dematerialize();
        }
    }

    public static final class CallableBuilder3<T1, T2, T3> {

        private final CallableBuilder b;
        private final Class<T1> cls1;
        private final Class<T2> cls2;
        private final Class<T3> cls3;

        public CallableBuilder3(CallableBuilder b, Class<T1> cls1, Class<T2> cls2, Class<T3> cls3) {
            this.b = b;
            this.cls1 = cls1;
            this.cls2 = cls2;
            this.cls3 = cls3;
        }

        public CallableBuilder3<T1, T2, T3> in(Flowable<?> f) {
            b.in(f);
            return this;
        }

        public CallableBuilder3<T1, T2, T3> in(Object... objects) {
            in(Flowable.fromArray(objects));
            return this;
        }
        
        public <T> CallableResultSets1Builder<T> map(Function<? super ResultSet, T> function) {
            return new CallableResultSets1Builder<T>(b, function);
        }

        public <T> CallableResultSets1Builder<T> autoMap(Class<T> cls) {
            return map(Util.autoMap(cls));
        }

        public Flowable<Tuple3<T1, T2, T3>> build() {
            return Call
                    .createWithThreeOutParameters(b.connection, b.sql, b.parameterGroups(), b.params, cls1, cls2, cls3) //
                    .dematerialize();
        }
    }
    
    public static final class CallableBuilder4<T1, T2, T3, T4> {

        private final CallableBuilder b;
        private final Class<T1> cls1;
        private final Class<T2> cls2;
        private final Class<T3> cls3;
        private final Class<T4> cls4;

        public CallableBuilder4(CallableBuilder b, Class<T1> cls1, Class<T2> cls2, Class<T3> cls3, Class<T4> cls4) {
            this.b = b;
            this.cls1 = cls1;
            this.cls2 = cls2;
            this.cls3 = cls3;
            this.cls4 = cls4;
        }

        public CallableBuilder4<T1, T2, T3, T4> in(Flowable<?> f) {
            b.in(f);
            return this;
        }

        public CallableBuilder4<T1, T2, T3, T4> in(Object... objects) {
            in(Flowable.fromArray(objects));
            return this;
        }
        
        public <T> CallableResultSets1Builder<T> map(Function<? super ResultSet, T> function) {
            return new CallableResultSets1Builder<T>(b, function);
        }

        public <T> CallableResultSets1Builder<T> autoMap(Class<T> cls) {
            return map(Util.autoMap(cls));
        }

        public Flowable<Tuple4<T1, T2, T3, T4>> build() {
            return Call
                    .createWithFourOutParameters(b.connection, b.sql, b.parameterGroups(), b.params, cls1, cls2, cls3, cls4) //
                    .dematerialize();
        }
    }

    public static final class CallableResultSets1Builder<T1> {

        private final CallableBuilder b;
        private final Function<? super ResultSet, ? extends T1> f1;

        CallableResultSets1Builder(CallableBuilder b, Function<? super ResultSet, ? extends T1> function) {
            this.b = b;
            this.f1 = function;
        }

        public <T2> CallableResultSets2Builder<T1, T2> autoMap(Class<T2> cls) {
            return map(Util.autoMap(cls));
        }

        public <T2> CallableResultSets2Builder<T1, T2> map(Function<? super ResultSet, ? extends T2> f2) {
            return new CallableResultSets2Builder<T1, T2>(b, f1, f2);
        }

        public CallableResultSets1Builder<T1> in(Flowable<?> f) {
            b.in(f);
            return this;
        }

        public CallableResultSets1Builder<T1> in(Object... objects) {
            return in(Flowable.fromArray(objects));
        }

        public Flowable<CallableResultSet1<T1>> build() {
            return Call.createWithOneResultSet(b.connection, b.sql, b.parameterGroups(), b.params, f1, 0) //
                    .dematerialize();
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

        public CallableResultSets2Builder<T1, T2> in(Flowable<?> f) {
            b.in(f);
            return this;
        }

        public CallableResultSets2Builder<T1, T2> in(Object... objects) {
            in(Flowable.fromArray(objects));
            return this;
        }

        public Flowable<CallableResultSet2<T1, T2>> build() {
            return Call.createWithTwoResultSets(b.connection, b.sql, b.parameterGroups(), b.params, f1, f2, 0) //
                    .dematerialize();
        }
    }

    public static final class CallableResultSet2<T1, T2> {

        private final List<Object> outs;
        private final Flowable<T1> query1;
        private final Flowable<T2> query2;

        public CallableResultSet2(List<Object> outs, Flowable<T1> query1, Flowable<T2> query2) {
            this.outs = outs;
            this.query1 = query1;
            this.query2 = query2;
        }

        public Flowable<T1> query1() {
            return query1;
        }

        public Flowable<T2> query2() {
            return query2;
        }

        public List<Object> outs() {
            return outs;
        }
    }

    public static final class CallableResultSet1<T1> {

        private final List<Object> outs;
        private final Flowable<T1> query1;

        public CallableResultSet1(List<Object> outs, Flowable<T1> query1) {
            this.outs = outs;
            this.query1 = query1;
        }

        public Flowable<T1> query1() {
            return query1;
        }

        public List<Object> outs() {
            return outs;
        }

    }

}
