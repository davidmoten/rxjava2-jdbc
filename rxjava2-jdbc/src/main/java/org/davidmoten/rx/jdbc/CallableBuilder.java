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
import org.davidmoten.rx.jdbc.tuple.TupleN;

import com.github.davidmoten.guavamini.Lists;
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

        public <T2> CallableBuilder2<T1, T2> inOut(Type type, Class<T2> cls2) {
            b.inOut(type, cls2);
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

        public <T3> CallableBuilder3<T1, T2, T3> inOut(Type type, Class<T3> cls3) {
            b.inOut(type, cls3);
            return new CallableBuilder3<T1, T2, T3>(b, cls1, cls2, cls3);
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

        public <T4> CallableBuilder4<T1, T2, T3, T4> out(Type type, Class<T4> cls4) {
            b.out(type, cls4);
            return new CallableBuilder4<T1, T2, T3, T4>(b, cls1, cls2, cls3, cls4);
        }

        public CallableBuilder3<T1, T2, T3> in(Flowable<?> f) {
            b.in(f);
            return this;
        }

        public CallableBuilder3<T1, T2, T3> in(Object... objects) {
            in(Flowable.fromArray(objects));
            return this;
        }

        public <T4> CallableBuilder4<T1, T2, T3, T4> inOut(Type type, Class<T4> cls4) {
            b.inOut(type, cls4);
            return new CallableBuilder4<T1, T2, T3, T4>(b, cls1, cls2, cls3, cls4);
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

        public CallableBuilderN inOut(Type type, Class<T3> cls5) {
            b.inOut(type, cls5);
            return new CallableBuilderN(b, Lists.newArrayList(cls1, cls2, cls3, cls4, cls5));
        }

        public CallableBuilderN out(Type type, Class<?> cls5) {
            b.out(type, cls5);
            return new CallableBuilderN(b, Lists.newArrayList(cls1, cls2, cls3, cls4, cls5));
        }

        public <T> CallableResultSets1Builder<T> map(Function<? super ResultSet, T> function) {
            return new CallableResultSets1Builder<T>(b, function);
        }

        public <T> CallableResultSets1Builder<T> autoMap(Class<T> cls) {
            return map(Util.autoMap(cls));
        }

        public Flowable<Tuple4<T1, T2, T3, T4>> build() {
            return Call
                    .createWithFourOutParameters(b.connection, b.sql, b.parameterGroups(), b.params, cls1, cls2, cls3,
                            cls4) //
                    .dematerialize();
        }
    }

    public static final class CallableBuilderN {

        private final CallableBuilder b;
        private final List<Class<?>> outClasses;

        public CallableBuilderN(CallableBuilder b, List<Class<?>> outClasses) {
            this.b = b;
            this.outClasses = outClasses;
        }

        public CallableBuilderN in(Flowable<?> f) {
            b.in(f);
            return this;
        }

        public CallableBuilderN in(Object... objects) {
            in(Flowable.fromArray(objects));
            return this;
        }

        public CallableBuilderN out(Type type, Class<?> cls) {
            b.out(type, cls);
            return new CallableBuilderN(b, createList(outClasses, cls));
        }

        public <T> CallableResultSets1Builder<T> map(Function<? super ResultSet, T> function) {
            return new CallableResultSets1Builder<T>(b, function);
        }

        public <T> CallableResultSets1Builder<T> autoMap(Class<T> cls) {
            return map(Util.autoMap(cls));
        }

        public Flowable<TupleN<Object>> build() {
            return Call.createWithNParameters(b.connection, b.sql, b.parameterGroups(), b.params, outClasses) //
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

        public CallableResultSets1Builder<T1> out(Type type, Class<?> cls5) {
            b.out(type, cls5);
            return this;
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

        public CallableResultSets1Builder<T1> inOut(Type type, Class<?> cls) {
            b.inOut(type, cls);
            return this;
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

        public CallableResultSets2Builder<T1, T2> out(Type type, Class<?> cls5) {
            b.out(type, cls5);
            return this;
        }

        public CallableResultSets2Builder<T1, T2> in(Flowable<?> f) {
            b.in(f);
            return this;
        }

        public CallableResultSets2Builder<T1, T2> in(Object... objects) {
            in(Flowable.fromArray(objects));
            return this;
        }

        public CallableResultSets2Builder<T1, T2> inOut(Type type, Class<?> cls) {
            b.inOut(type, cls);
            return this;
        }

        public <T3> CallableResultSets3Builder<T1, T2, T3> autoMap(Class<T3> cls) {
            return map(Util.autoMap(cls));
        }

        public <T3> CallableResultSets3Builder<T1, T2, T3> map(Function<? super ResultSet, ? extends T3> f3) {
            return new CallableResultSets3Builder<T1, T2, T3>(b, f1, f2, f3);
        }

        public Flowable<CallableResultSet2<T1, T2>> build() {
            return Call.createWithTwoResultSets(b.connection, b.sql, b.parameterGroups(), b.params, f1, f2, 0) //
                    .dematerialize();
        }
    }

    public static final class CallableResultSets3Builder<T1, T2, T3> {

        private final CallableBuilder b;
        private final Function<? super ResultSet, ? extends T1> f1;
        private final Function<? super ResultSet, ? extends T2> f2;
        private final Function<? super ResultSet, ? extends T3> f3;

        CallableResultSets3Builder(CallableBuilder b, Function<? super ResultSet, ? extends T1> f1,
                Function<? super ResultSet, ? extends T2> f2, Function<? super ResultSet, ? extends T3> f3) {
            this.b = b;
            this.f1 = f1;
            this.f2 = f2;
            this.f3 = f3;
        }

        public CallableResultSets3Builder<T1, T2, T3> out(Type type, Class<?> cls5) {
            b.out(type, cls5);
            return this;
        }

        public CallableResultSets3Builder<T1, T2, T3> in(Flowable<?> f) {
            b.in(f);
            return this;
        }

        public CallableResultSets3Builder<T1, T2, T3> in(Object... objects) {
            in(Flowable.fromArray(objects));
            return this;
        }

        public CallableResultSets3Builder<T1, T2, T3> inOut(Type type, Class<?> cls) {
            b.inOut(type, cls);
            return this;
        }

        public <T> CallableResultSetsNBuilder autoMap(Class<T> cls) {
            return map(Util.autoMap(cls));
        }

        @SuppressWarnings("unchecked")
        public CallableResultSetsNBuilder map(Function<? super ResultSet, ?> f) {
            return new CallableResultSetsNBuilder(b, Lists.newArrayList(f1, f2, f3, f));
        }

        public Flowable<CallableResultSet3<T1, T2, T3>> build() {
            return Call.createWithThreeResultSets(b.connection, b.sql, b.parameterGroups(), b.params, f1, f2, f3, 0) //
                    .dematerialize();
        }
    }

    public static final class CallableResultSetsNBuilder {

        private final CallableBuilder b;
        private final List<Function<? super ResultSet, ?>> functions;

        CallableResultSetsNBuilder(CallableBuilder b, List<Function<? super ResultSet, ?>> functions) {
            this.b = b;
            this.functions = functions;
        }

        public CallableResultSetsNBuilder in(Flowable<?> f) {
            b.in(f);
            return this;
        }

        public CallableResultSetsNBuilder in(Object... objects) {
            in(Flowable.fromArray(objects));
            return this;
        }

        public CallableResultSetsNBuilder inOut(Type type, Class<?> cls) {
            b.inOut(type, cls);
            return this;
        }

        public <T> CallableResultSetsNBuilder autoMap(Class<T> cls) {
            return map(Util.autoMap(cls));
        }

        public CallableResultSetsNBuilder map(Function<? super ResultSet, ?> f) {
            functions.add(f);
            return this;
        }

        public Flowable<CallableResultSetN> build() {
            return Call.createWithNResultSets(b.connection, b.sql, b.parameterGroups(), b.params, functions, 0) //
                    .dematerialize();
        }
    }

    public static final class CallableResultSet1<T1> {

        private final List<Object> outs;
        private final Flowable<T1> results;

        public CallableResultSet1(List<Object> outs, Flowable<T1> results) {
            this.outs = outs;
            this.results = results;
        }

        public Flowable<T1> results() {
            return results;
        }

        public List<Object> outs() {
            return outs;
        }

    }

    public static final class CallableResultSet2<T1, T2> {

        private final List<Object> outs;
        private final Flowable<T1> results1;
        private final Flowable<T2> results2;

        public CallableResultSet2(List<Object> outs, Flowable<T1> results1, Flowable<T2> results2) {
            this.outs = outs;
            this.results1 = results1;
            this.results2 = results2;
        }

        public Flowable<T1> results1() {
            return results1;
        }

        public Flowable<T2> results2() {
            return results2;
        }

        public List<Object> outs() {
            return outs;
        }
    }

    public static final class CallableResultSet3<T1, T2, T3> {

        private final List<Object> outs;
        private final Flowable<T1> results1;
        private final Flowable<T2> results2;
        private final Flowable<T3> results3;

        public CallableResultSet3(List<Object> outs, Flowable<T1> query1, Flowable<T2> query2, Flowable<T3> query3) {
            this.outs = outs;
            this.results1 = query1;
            this.results2 = query2;
            this.results3 = query3;
        }

        public Flowable<T1> results1() {
            return results1;
        }

        public Flowable<T2> results2() {
            return results2;
        }

        public Flowable<T3> results3() {
            return results3;
        }

        public List<Object> outs() {
            return outs;
        }
    }

    public static final class CallableResultSetN {

        private final List<Object> outs;
        private final List<Flowable<?>> flowables;

        public CallableResultSetN(List<Object> outs, List<Flowable<?>> flowables) {
            this.outs = outs;
            this.flowables = flowables;
        }

        public List<Flowable<?>> results() {
            return flowables;
        }

        public List<Object> outs() {
            return outs;
        }
    }

    private static <T> List<T> createList(List<T> list, T t) {
        ArrayList<T> r = new ArrayList<>(list);
        r.add(t);
        return r;
    }

}
