package org.davidmoten.rx.jdbc;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

import org.davidmoten.rx.jdbc.annotations.Column;
import org.davidmoten.rx.jdbc.tuple.Tuple2;

import com.github.davidmoten.guavamini.Preconditions;

import io.reactivex.Completable;
import io.reactivex.Flowable;
import io.reactivex.Single;
import io.reactivex.functions.Function;

public class CallableBuilder {

    final String sql;
    final List<Object> in = new ArrayList<>();
    Flowable<?> inStream;

    public CallableBuilder(String sql) {
        this.sql = sql;
    }

    public Completable perform() {
        // TODO
        return null;
    }

    public CallableBuilder in(Object o) {
        in.add(o);
        return this;
    }

    public CallableBuilder in(Flowable<?> f) {
        Preconditions.checkArgument(in.isEmpty(), "you can explicitly specify in parameters or a stream, not both");
        Preconditions.checkArgument(inStream == null, "you can only specify in flowable once");
        this.inStream = f;
        return this;
    }

    public <T> CallableBuilder1<T> inOut(T o, Class<T> cls) {
        in.add(o);
        return new CallableBuilder1<T>(this, cls);
    }

    public <T> CallableBuilder1<T> out(Class<T> cls) {
        return new CallableBuilder1<T>(this, cls);
    }

    public <T> CallableResultSets1Builder<T> autoMap(Class<T> cls) {
        return new CallableResultSets1Builder<T>(this, Util.autoMap(cls));
    }

    public static final class CallableBuilder1<T1> {

        private final CallableBuilder b;
        private final Class<T1> cls;

        public CallableBuilder1(CallableBuilder b, Class<T1> cls) {
            this.b = b;
            this.cls = cls;
        }

        public CallableBuilder1<T1> in(Object o) {
            b.in.add(o);
            return this;
        }

        public <T2> CallableBuilder2<T1, T2> out(Class<T2> cls2) {
            return new CallableBuilder2<T1, T2>(b, cls, cls2);
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

    private interface Person {
        @Column
        String name();

        @Column
        int score();
    }

    private static CallableBuilder call(String sql) {
        return new CallableBuilder(sql);
    }

    public static void main(String[] args) {
        {
            // in and out parameters are ordered by position in sql
            Single<Tuple2<Integer, String>> result = call("call doit(?,?,?,?)") //
                    .in(10) //
                    .inOut(5, Integer.class) //
                    .out(String.class) //
                    .build();
        }
        {
            // result set returns don't have parameters and are at the end of the java
            // procedure declaration (can this position vary?)
            Single<CallableResultSet2<Person, Person>> result = call("call doit(?)") //
                    .in(10) //
                    .autoMap(Person.class) //
                    .autoMap(Person.class) //
                    .build();
            result.flatMapPublisher( //
                    r -> r.query1() //
                            .mergeWith(r.query2())) //
                    .count() //
                    .doOnSuccess(System.out::println) //
                    .blockingGet();
        }
    }

}
