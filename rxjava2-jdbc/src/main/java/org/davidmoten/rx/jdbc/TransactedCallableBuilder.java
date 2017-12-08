package org.davidmoten.rx.jdbc;

import java.sql.Connection;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import org.davidmoten.rx.jdbc.callable.internal.In;
import org.davidmoten.rx.jdbc.callable.internal.InOut;
import org.davidmoten.rx.jdbc.callable.internal.InParameterPlaceholder;
import org.davidmoten.rx.jdbc.callable.internal.Out;
import org.davidmoten.rx.jdbc.callable.internal.TxGetter1;
import org.davidmoten.rx.jdbc.callable.internal.TxGetter2;
import org.davidmoten.rx.jdbc.callable.internal.TxGetter3;
import org.davidmoten.rx.jdbc.callable.internal.TxGetter4;
import org.davidmoten.rx.jdbc.callable.internal.TxGetterN;
import org.davidmoten.rx.jdbc.tuple.Tuple2;
import org.davidmoten.rx.jdbc.tuple.Tuple3;
import org.davidmoten.rx.jdbc.tuple.Tuple4;
import org.davidmoten.rx.jdbc.tuple.TupleN;

import com.github.davidmoten.guavamini.Lists;
import com.github.davidmoten.guavamini.Preconditions;

import io.reactivex.Flowable;
import io.reactivex.Single;
import io.reactivex.functions.Function;

public final class TransactedCallableBuilder implements TxGetter1 {

    private CallableBuilder cb;

    public TransactedCallableBuilder(CallableBuilder cb) {
        this.cb = cb;
    }

    @SuppressWarnings("unchecked")
    public Flowable<List<Object>> parameterGroups() {
        int numInParameters =cb. params.stream() //
                .filter(x -> x instanceof InParameterPlaceholder) //
                .collect(Collectors.counting()).intValue();
        if (numInParameters == 0) {
            return cb.inStream.map(x -> Collections.singletonList(x));
        } else {
            return (Flowable<List<Object>>) (Flowable<?>) cb.inStream.buffer(numInParameters);
        }
    }

    public TransactedCallableBuilder in() {
        cb.params.add(In.IN);
        return this;
    }

    public Single<TxWithoutValue> in(Flowable<?> f) {
        Preconditions.checkArgument(cb.inStream == null, "you can only specify in flowable once, current=" + cb.inStream);
        cb.inStream = f;
        return build();
    }

    public Single<TxWithoutValue> once() {
        return in(1);
    }

    public Single<TxWithoutValue> in(Object... objects) {
        return in(Flowable.fromArray(objects));
    }

    public <T> CallableBuilder1<T> inOut(Type type, Class<T> cls) {
        cb.params.add(new InOut(type, cls));
        return new CallableBuilder1<T>(cb, cls);
    }

    public <T> CallableBuilder1<T> out(Type type, Class<T> cls) {
        cb.params.add(new Out(type, cls));
        return new CallableBuilder1<T>(cb, cls);
    }

    @Override
    public <T> CallableResultSets1Builder<T> get(Function<? super ResultSet, ? extends T> function) {
        return new CallableResultSets1Builder<T>(cb, function);
    }

    public <T> CallableResultSets1Builder<T> autoMap(Class<T> cls) {
        return get(Util.autoMap(cls));
    }

    @SuppressWarnings("unchecked")
    private Single<TxWithoutValue> build() {
        return Single.defer(() -> {
            AtomicReference<Connection> con = new AtomicReference<Connection>();
            // set the atomic reference when transactedConnection emits
            Single<Connection> transactedConnection = cb.connection //
                    .map(c -> Util.toTransactedConnection(con, c));
            return Call //
                    .createWithZeroOutParameters(transactedConnection, cb.sql, parameterGroups(), cb.params) //
                    .materialize() //
                    .filter(x -> !x.isOnNext()) //
                    .<TxWithoutValue>flatMap(n -> Tx.toTx(n, con.get(), cb.db)) //
                    .doOnNext(tx -> {
                        if (tx.isComplete()) {
                            ((TxImpl<Object>) tx).connection().commit();
                        }
                    }) //
                    .lastOrError();
        });
    }

    public static final class CallableBuilder1<T1> implements TxGetter1 {

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

        public Flowable<T1> in(Flowable<?> f) {
            b.in(f);
            return build();
        }

        public Flowable<T1> in(Object... objects) {
            return in(Flowable.fromArray(objects));
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

    public static final class CallableBuilder2<T1, T2> implements TxGetter1 {

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

        public Flowable<Tuple2<T1, T2>> in(Flowable<?> f) {
            b.in(f);
            return build();
        }

        public CallableBuilder2<T1, T2> in() {
            b.in();
            return this;
        }

        public Flowable<Tuple2<T1, T2>> in(Object... objects) {
            return in(Flowable.fromArray(objects));
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

    public static final class CallableBuilder3<T1, T2, T3> implements TxGetter1 {

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

        public Flowable<Tuple3<T1, T2, T3>> in(Flowable<?> f) {
            b.in(f);
            return build();
        }

        public CallableBuilder3<T1, T2, T3> in() {
            b.in();
            return this;
        }

        public Flowable<Tuple3<T1, T2, T3>> in(Object... objects) {
            return in(Flowable.fromArray(objects));
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

    public static final class CallableBuilder4<T1, T2, T3, T4> implements TxGetter1 {

        private final CallableBuilder b;
        private final Class<T1> cls1;
        private final Class<T2> cls2;
        private final Class<T3> cls3;
        private final Class<T4> cls4;

        public CallableBuilder4(CallableBuilder b, Class<T1> cls1, Class<T2> cls2, Class<T3> cls3,
                Class<T4> cls4) {
            this.b = b;
            this.cls1 = cls1;
            this.cls2 = cls2;
            this.cls3 = cls3;
            this.cls4 = cls4;
        }

        public Flowable<Tuple4<T1, T2, T3, T4>> in(Flowable<?> f) {
            b.in(f);
            return build();
        }

        public CallableBuilder4<T1, T2, T3, T4> in() {
            b.in();
            return this;
        }

        public Flowable<Tuple4<T1, T2, T3, T4>> in(Object... objects) {
            return in(Flowable.fromArray(objects));
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

    public static final class CallableBuilderN implements TxGetter1 {

        private final CallableBuilder b;
        private final List<Class<?>> outClasses;

        public CallableBuilderN(CallableBuilder b, List<Class<?>> outClasses) {
            this.b = b;
            this.outClasses = outClasses;
        }

        public Flowable<TupleN<Object>> in(Flowable<?> f) {
            b.in(f);
            return build();
        }

        public CallableBuilderN in() {
            b.in();
            return this;
        }

        public Flowable<TupleN<Object>> in(Object... objects) {
            return in(Flowable.fromArray(objects));
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

    public static final class CallableResultSets1Builder<T1> implements TxGetter2<T1> {

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

        public Flowable<CallableResultSet1<T1>> in(Flowable<?> f) {
            b.in(f);
            return build();
        }

        public CallableResultSets1Builder<T1> in() {
            b.in();
            return this;
        }

        public Flowable<CallableResultSet1<T1>> in(Object... objects) {
            return in(Flowable.fromArray(objects));
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

    public static final class CallableResultSets2Builder<T1, T2> implements TxGetter3<T1, T2> {

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

        public Flowable<CallableResultSet2<T1, T2>> in(Flowable<?> f) {
            b.in(f);
            return build();
        }

        public CallableResultSets2Builder<T1, T2> in() {
            b.in();
            return this;
        }

        public Flowable<CallableResultSet2<T1, T2>> in(Object... objects) {
            return in(Flowable.fromArray(objects));
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

    public static final class CallableResultSets3Builder<T1, T2, T3> implements TxGetter4<T1, T2, T3> {

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

        public Flowable<CallableResultSet3<T1, T2, T3>> in(Flowable<?> f) {
            b.in(f);
            return build();
        }

        public Flowable<CallableResultSet3<T1, T2, T3>> in(Object... objects) {
            return in(Flowable.fromArray(objects));
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

    public static final class CallableResultSets4Builder<T1, T2, T3, T4> implements TxGetterN {

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

        public Flowable<CallableResultSet4<T1, T2, T3, T4>> in(Flowable<?> f) {
            b.in(f);
            return build();
        }

        public Flowable<CallableResultSet4<T1, T2, T3, T4>> in(Object... objects) {
            return in(Flowable.fromArray(objects));
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

        public Flowable<CallableResultSetN> in(Flowable<?> f) {
            b.in(f);
            return build();
        }

        public Flowable<CallableResultSetN> in(Object... objects) {
            return in(Flowable.fromArray(objects));
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

    public static final class CallableResultSet4<T1, T2, T3, T4> {

        private final List<Object> outs;
        private final Flowable<T1> results1;
        private final Flowable<T2> results2;
        private final Flowable<T3> results3;
        private final Flowable<T4> results4;

        public CallableResultSet4(List<Object> outs, Flowable<T1> query1, Flowable<T2> query2, Flowable<T3> query3,
                Flowable<T4> query4) {
            this.outs = outs;
            this.results1 = query1;
            this.results2 = query2;
            this.results3 = query3;
            this.results4 = query4;
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

        public Flowable<T4> results4() {
            return results4;
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

        public Flowable<?> results(int index) {
            return flowables.get(index);
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
