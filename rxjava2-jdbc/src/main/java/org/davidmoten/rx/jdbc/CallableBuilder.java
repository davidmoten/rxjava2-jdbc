package org.davidmoten.rx.jdbc;

import java.sql.Connection;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

import org.davidmoten.rx.jdbc.tuple.Tuple2;

import com.github.davidmoten.guavamini.Preconditions;

import io.reactivex.Completable;
import io.reactivex.Flowable;
import io.reactivex.Single;
import io.reactivex.functions.Function;

public class CallableBuilder {

    private static final Object IN_SENTINEL = new Object();

    final String sql;
    final List<Object> params = new ArrayList<>();

    Flowable<?> inStream;

    public CallableBuilder(String sql, Single<Connection> connection) {
        this.sql = sql;
    }

    static final class InOut {
        final Type type;
        final Class<?> cls;

        InOut(Type type, Class<?> cls) {
            this.type = type;
            this.cls = cls;
        }
    }

    public Completable perform() {
        // TODO
        return null;
    }

    public CallableBuilder in() {
        params.add(IN_SENTINEL);
        return this;
    }

    public CallableBuilder in(Flowable<?> f) {
        Preconditions.checkArgument(inStream == null, "you can only specify in flowable once");
        this.inStream = f;
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
        return new CallableResultSets1Builder<T>(this, Util.autoMap(cls));
    }

    // TODO restrict visibility
    public static final class Out {
        final Type type;
        final Class<?> cls;

        public Out(Type type, Class<?> cls) {
            this.type = type;
            this.cls = cls;
        }
    }

    public static final class CallableBuilder1<T1> {

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
            b.out(type,  cls2);
            return new CallableBuilder2<T1, T2>(b, cls, cls2);
        }

        public Single<T1> build() {
            return null;
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

        public Single<Tuple2<T1, T2>> build() {

            // TODO Auto-generated method stub
            return null;
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
            return new CallableResultSets2Builder<T1, T2>(b, f1, Util.autoMap(cls));
        }

        public <T2> CallableResultSets2Builder<T1, T2> map(Function<? super ResultSet, ? extends T2> f2) {
            return new CallableResultSets2Builder<T1, T2>(b, f1, f2);
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

}
