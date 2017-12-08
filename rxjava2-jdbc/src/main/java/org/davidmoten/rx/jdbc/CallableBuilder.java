package org.davidmoten.rx.jdbc;

import java.sql.Connection;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import org.davidmoten.rx.jdbc.callable.CallableResultSet1;
import org.davidmoten.rx.jdbc.callable.CallableResultSet2;
import org.davidmoten.rx.jdbc.callable.CallableResultSet3;
import org.davidmoten.rx.jdbc.callable.CallableResultSet4;
import org.davidmoten.rx.jdbc.callable.CallableResultSetN;
import org.davidmoten.rx.jdbc.callable.internal.Getter1;
import org.davidmoten.rx.jdbc.callable.internal.Getter2;
import org.davidmoten.rx.jdbc.callable.internal.Getter3;
import org.davidmoten.rx.jdbc.callable.internal.Getter4;
import org.davidmoten.rx.jdbc.callable.internal.GetterN;
import org.davidmoten.rx.jdbc.callable.internal.In;
import org.davidmoten.rx.jdbc.callable.internal.InOut;
import org.davidmoten.rx.jdbc.callable.internal.InParameterPlaceholder;
import org.davidmoten.rx.jdbc.callable.internal.Out;
import org.davidmoten.rx.jdbc.callable.internal.ParameterPlaceholder;
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

public final class CallableBuilder implements Getter1 {

    final String sql;
    final List<ParameterPlaceholder> params = new ArrayList<>();

    Flowable<?> inStream;

    final Single<Connection> connection;
    final Database db;

    public CallableBuilder(String sql, Single<Connection> connection, Database db) {
        this.sql = sql;
        this.connection = connection;
        this.db = db;
    }

    public TransactedCallableBuilder transacted() {
        return new TransactedCallableBuilder(this);
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

    public CallableBuilder in() {
        params.add(In.IN);
        return this;
    }

    public Completable input(Flowable<?> f) {
        Preconditions.checkArgument(inStream == null, "you can only specify in flowable once, current=" + inStream);
        this.inStream = f;
        return build();
    }

    public Completable once() {
        return input(1);
    }

    public Completable input(Object... objects) {
        return input(Flowable.fromArray(objects));
    }

    public <T> CallableBuilder1<T> inOut(Type type, Class<T> cls) {
        params.add(new InOut(type, cls));
        return new CallableBuilder1<T>(this, cls);
    }

    public <T> CallableBuilder1<T> out(Type type, Class<T> cls) {
        params.add(new Out(type, cls));
        return new CallableBuilder1<T>(this, cls);
    }

    @Override
    public <T> CallableResultSets1Builder<T> get(Function<? super ResultSet, ? extends T> function) {
        return new CallableResultSets1Builder<T>(this, function);
    }

    public <T> CallableResultSets1Builder<T> autoMap(Class<T> cls) {
        return get(Util.autoMap(cls));
    }

    private Completable build() {
        return Call.createWithZeroOutParameters(connection, sql, parameterGroups(), params).ignoreElements();
    }

    public static final class CallableBuilder1<T1> implements Getter1 {

        private final CallableBuilder b;
        private final Class<T1> cls;

        public CallableBuilder1(CallableBuilder b, Class<T1> cls) {
            this.b = b;
            this.cls = cls;
        }

        public CallableBuilder1<T1> in() {
            b.in();
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

        public Flowable<T1> input(Flowable<?> f) {
            b.input(f);
            return build();
        }

        public Flowable<T1> input(Object... objects) {
            return input(Flowable.fromArray(objects));
        }

        @Override
        public <T> CallableResultSets1Builder<T> get(Function<? super ResultSet, ? extends T> function) {
            return new CallableResultSets1Builder<T>(b, function);
        }

        public <T> CallableResultSets1Builder<T> autoMap(Class<T> cls) {
            return get(Util.autoMap(cls));
        }

        private Flowable<T1> build() {
            return Call.createWithOneOutParameter(b.connection, b.sql, b.parameterGroups(), b.params, cls) //
                    .dematerialize();
        }
    }

    public static final class CallableBuilder2<T1, T2> implements Getter1 {

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

        public Flowable<Tuple2<T1, T2>> input(Flowable<?> f) {
            b.input(f);
            return build();
        }

        public CallableBuilder2<T1, T2> in() {
            b.in();
            return this;
        }

        public Flowable<Tuple2<T1, T2>> input(Object... objects) {
            return input(Flowable.fromArray(objects));
        }

        public <T3> CallableBuilder3<T1, T2, T3> inOut(Type type, Class<T3> cls3) {
            b.inOut(type, cls3);
            return new CallableBuilder3<T1, T2, T3>(b, cls1, cls2, cls3);
        }

        @Override
        public <T> CallableResultSets1Builder<T> get(Function<? super ResultSet, ? extends T> function) {
            return new CallableResultSets1Builder<T>(b, function);
        }

        public <T> CallableResultSets1Builder<T> autoMap(Class<T> cls) {
            return get(Util.autoMap(cls));
        }

        private Flowable<Tuple2<T1, T2>> build() {
            return Call.createWithTwoOutParameters(b.connection, b.sql, b.parameterGroups(), b.params, cls1, cls2) //
                    .dematerialize();
        }
    }

    public static final class CallableBuilder3<T1, T2, T3> implements Getter1 {

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

        public Flowable<Tuple3<T1, T2, T3>> input(Flowable<?> f) {
            b.input(f);
            return build();
        }

        public CallableBuilder3<T1, T2, T3> in() {
            b.in();
            return this;
        }

        public Flowable<Tuple3<T1, T2, T3>> input(Object... objects) {
            return input(Flowable.fromArray(objects));
        }

        public <T4> CallableBuilder4<T1, T2, T3, T4> inOut(Type type, Class<T4> cls4) {
            b.inOut(type, cls4);
            return new CallableBuilder4<T1, T2, T3, T4>(b, cls1, cls2, cls3, cls4);
        }

        @Override
        public <T> CallableResultSets1Builder<T> get(Function<? super ResultSet, ? extends T> function) {
            return new CallableResultSets1Builder<T>(b, function);
        }

        public <T> CallableResultSets1Builder<T> autoMap(Class<T> cls) {
            return get(Util.autoMap(cls));
        }

        private Flowable<Tuple3<T1, T2, T3>> build() {
            return Call
                    .createWithThreeOutParameters(b.connection, b.sql, b.parameterGroups(), b.params, cls1, cls2, cls3) //
                    .dematerialize();
        }
    }

    public static final class CallableBuilder4<T1, T2, T3, T4> implements Getter1 {

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

        public Flowable<Tuple4<T1, T2, T3, T4>> input(Flowable<?> f) {
            b.input(f);
            return build();
        }

        public CallableBuilder4<T1, T2, T3, T4> in() {
            b.in();
            return this;
        }

        public Flowable<Tuple4<T1, T2, T3, T4>> input(Object... objects) {
            return input(Flowable.fromArray(objects));
        }

        public CallableBuilderN inOut(Type type, Class<T3> cls5) {
            b.inOut(type, cls5);
            return new CallableBuilderN(b, Lists.newArrayList(cls1, cls2, cls3, cls4, cls5));
        }

        public CallableBuilderN out(Type type, Class<?> cls5) {
            b.out(type, cls5);
            return new CallableBuilderN(b, Lists.newArrayList(cls1, cls2, cls3, cls4, cls5));
        }

        @Override
        public <T> CallableResultSets1Builder<T> get(Function<? super ResultSet, ? extends T> function) {
            return new CallableResultSets1Builder<T>(b, function);
        }

        public <T> CallableResultSets1Builder<T> autoMap(Class<T> cls) {
            return get(Util.autoMap(cls));
        }

        private Flowable<Tuple4<T1, T2, T3, T4>> build() {
            return Call
                    .createWithFourOutParameters(b.connection, b.sql, b.parameterGroups(), b.params, cls1, cls2, cls3,
                            cls4) //
                    .dematerialize();
        }
    }

    public static final class CallableBuilderN implements Getter1 {

        private final CallableBuilder b;
        private final List<Class<?>> outClasses;

        public CallableBuilderN(CallableBuilder b, List<Class<?>> outClasses) {
            this.b = b;
            this.outClasses = outClasses;
        }

        public Flowable<TupleN<Object>> input(Flowable<?> f) {
            b.input(f);
            return build();
        }

        public CallableBuilderN in() {
            b.in();
            return this;
        }

        public Flowable<TupleN<Object>> input(Object... objects) {
            return input(Flowable.fromArray(objects));
        }

        public CallableBuilderN out(Type type, Class<?> cls) {
            b.out(type, cls);
            return new CallableBuilderN(b, createList(outClasses, cls));
        }

        @Override
        public <T> CallableResultSets1Builder<T> get(Function<? super ResultSet, ? extends T> function) {
            return new CallableResultSets1Builder<T>(b, function);
        }

        public <T> CallableResultSets1Builder<T> autoMap(Class<T> cls) {
            return get(Util.autoMap(cls));
        }

        private Flowable<TupleN<Object>> build() {
            return Call.createWithNParameters(b.connection, b.sql, b.parameterGroups(), b.params, outClasses) //
                    .dematerialize();
        }

    }

    public static final class CallableResultSets1Builder<T1> implements Getter2<T1> {

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
            return get(Util.autoMap(cls));
        }

        public <T2> CallableResultSets2Builder<T1, T2> get(Function<? super ResultSet, ? extends T2> f2) {
            return new CallableResultSets2Builder<T1, T2>(b, f1, f2);
        }

        public Flowable<CallableResultSet1<T1>> input(Flowable<?> f) {
            b.input(f);
            return build();
        }

        public CallableResultSets1Builder<T1> in() {
            b.in();
            return this;
        }

        public Flowable<CallableResultSet1<T1>> input(Object... objects) {
            return input(Flowable.fromArray(objects));
        }

        public CallableResultSets1Builder<T1> inOut(Type type, Class<?> cls) {
            b.inOut(type, cls);
            return this;
        }

        private Flowable<CallableResultSet1<T1>> build() {
            return Call.createWithOneResultSet(b.connection, b.sql, b.parameterGroups(), b.params, f1, 0) //
                    .dematerialize();
        }

    }

    public static final class CallableResultSets2Builder<T1, T2> implements Getter3<T1, T2> {

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

        public Flowable<CallableResultSet2<T1, T2>> input(Flowable<?> f) {
            b.input(f);
            return build();
        }

        public CallableResultSets2Builder<T1, T2> in() {
            b.in();
            return this;
        }

        public Flowable<CallableResultSet2<T1, T2>> input(Object... objects) {
            return input(Flowable.fromArray(objects));
        }

        public CallableResultSets2Builder<T1, T2> inOut(Type type, Class<?> cls) {
            b.inOut(type, cls);
            return this;
        }

        public <T3> CallableResultSets3Builder<T1, T2, T3> autoMap(Class<T3> cls) {
            return get(Util.autoMap(cls));
        }

        public <T3> CallableResultSets3Builder<T1, T2, T3> get(Function<? super ResultSet, ? extends T3> f3) {
            return new CallableResultSets3Builder<T1, T2, T3>(b, f1, f2, f3);
        }

        private Flowable<CallableResultSet2<T1, T2>> build() {
            return Call.createWithTwoResultSets(b.connection, b.sql, b.parameterGroups(), b.params, f1, f2, 0) //
                    .dematerialize();
        }
    }

    public static final class CallableResultSets3Builder<T1, T2, T3> implements Getter4<T1, T2, T3> {

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

        public CallableResultSets3Builder<T1, T2, T3> in() {
            b.in();
            return this;
        }

        public Flowable<CallableResultSet3<T1, T2, T3>> input(Flowable<?> f) {
            b.input(f);
            return build();
        }

        public Flowable<CallableResultSet3<T1, T2, T3>> input(Object... objects) {
            return input(Flowable.fromArray(objects));
        }

        public CallableResultSets3Builder<T1, T2, T3> inOut(Type type, Class<?> cls) {
            b.inOut(type, cls);
            return this;
        }

        public <T4> CallableResultSets4Builder<T1, T2, T3, T4> autoMap(Class<T4> cls) {
            return get(Util.autoMap(cls));
        }

        public <T4> CallableResultSets4Builder<T1, T2, T3, T4> get(Function<? super ResultSet, ? extends T4> f4) {
            return new CallableResultSets4Builder<T1, T2, T3, T4>(b, f1, f2, f3, f4);
        }

        private Flowable<CallableResultSet3<T1, T2, T3>> build() {
            return Call.createWithThreeResultSets(b.connection, b.sql, b.parameterGroups(), b.params, f1, f2, f3, 0) //
                    .dematerialize();
        }
    }

    public static final class CallableResultSets4Builder<T1, T2, T3, T4> implements GetterN {

        private final CallableBuilder b;
        private final Function<? super ResultSet, ? extends T1> f1;
        private final Function<? super ResultSet, ? extends T2> f2;
        private final Function<? super ResultSet, ? extends T3> f3;
        private final Function<? super ResultSet, ? extends T4> f4;

        CallableResultSets4Builder(CallableBuilder b, Function<? super ResultSet, ? extends T1> f1,
                Function<? super ResultSet, ? extends T2> f2, Function<? super ResultSet, ? extends T3> f3,
                Function<? super ResultSet, ? extends T4> f4) {
            this.b = b;
            this.f1 = f1;
            this.f2 = f2;
            this.f3 = f3;
            this.f4 = f4;
        }

        public CallableResultSets4Builder<T1, T2, T3, T4> out(Type type, Class<?> cls5) {
            b.out(type, cls5);
            return this;
        }

        public CallableResultSets4Builder<T1, T2, T3, T4> in() {
            b.in();
            return this;
        }

        public Flowable<CallableResultSet4<T1, T2, T3, T4>> input(Flowable<?> f) {
            b.input(f);
            return build();
        }

        public Flowable<CallableResultSet4<T1, T2, T3, T4>> input(Object... objects) {
            return input(Flowable.fromArray(objects));
        }

        public CallableResultSets4Builder<T1, T2, T3, T4> inOut(Type type, Class<?> cls) {
            b.inOut(type, cls);
            return this;
        }

        public <T> CallableResultSetsNBuilder autoMap(Class<T> cls) {
            return get(Util.autoMap(cls));
        }

        @SuppressWarnings("unchecked")
        public CallableResultSetsNBuilder get(Function<? super ResultSet, ?> f5) {
            return new CallableResultSetsNBuilder(b, Lists.newArrayList(f1, f2, f3, f4, f5));
        }

        public Flowable<CallableResultSet4<T1, T2, T3, T4>> build() {
            return Call.createWithFourResultSets(b.connection, b.sql, b.parameterGroups(), b.params, f1, f2, f3, f4, 0)
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

        public CallableResultSetsNBuilder in() {
            b.in();
            return this;
        }

        public Flowable<CallableResultSetN> input(Flowable<?> f) {
            b.input(f);
            return build();
        }

        public Flowable<CallableResultSetN> input(Object... objects) {
            return input(Flowable.fromArray(objects));
        }

        public CallableResultSetsNBuilder inOut(Type type, Class<?> cls) {
            b.inOut(type, cls);
            return this;
        }

        public <T> CallableResultSetsNBuilder autoMap(Class<T> cls) {
            return get(Util.autoMap(cls));
        }

        public CallableResultSetsNBuilder get(Function<? super ResultSet, ?> f) {
            functions.add(f);
            return this;
        }

        private Flowable<CallableResultSetN> build() {
            return Call.createWithNResultSets(b.connection, b.sql, b.parameterGroups(), b.params, functions, 0) //
                    .dematerialize();
        }
    }

    private static <T> List<T> createList(List<T> list, T t) {
        ArrayList<T> r = new ArrayList<>(list);
        r.add(t);
        return r;
    }

}
