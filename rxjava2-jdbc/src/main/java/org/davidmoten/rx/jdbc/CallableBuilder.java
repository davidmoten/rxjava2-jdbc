package org.davidmoten.rx.jdbc;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

import org.davidmoten.rx.jdbc.annotations.Column;
import org.davidmoten.rx.jdbc.tuple.Tuple2;

import io.reactivex.Completable;
import io.reactivex.Flowable;
import io.reactivex.Single;
import io.reactivex.functions.Function;

public class CallableBuilder {

    private final String sql;
    private final List<Object> in = new ArrayList<>();

    // db.call(sql)
    // .parameter(0)
    // .out(Integer.class) - at this point returns typed builder
    // .autoMap(Person.class) - abandon typing of out parameters in favour of
    // ResultSets
    // .map(rs -> {new Person(rs.getString(1), rs.getInt(2))
    // .get();

    // db.call(sql)
    // .perform() - returns Completable

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

    public <T> CallableBuilder1<T> inOut(T o, Class<T> cls) {
        in.add(o);
        return new CallableBuilder1<T>(in, cls);
    }

    public <T> CallableBuilder1<T> out(Class<T> cls) {
        return new CallableBuilder1<T>(in, cls);
    }

    public <T> CallableResultSets1Builder<T> autoMap(Class<T> cls) {
        return new CallableResultSets1Builder<T>(in, Util.autoMap(cls));
    }

    public static final class CallableBuilder1<T1> {

        private final List<Object> in;
        private final Class<T1> cls;

        public CallableBuilder1(List<Object> in, Class<T1> cls) {
            this.in = in;
            this.cls = cls;
        }

        public CallableBuilder1<T1> in(Object o) {
            in.add(o);
            return this;
        }

        public <T2> CallableBuilder2<T1, T2> out(Class<T2> cls2) {
            return new CallableBuilder2<T1, T2>(in, cls, cls2);
        }
    }

    public static final class CallableBuilder2<T1, T2> {

        private final List<Object> in;
        private final Class<T1> cls1;
        private final Class<T2> cls2;

        public CallableBuilder2(List<Object> in, Class<T1> cls1, Class<T2> cls2) {
            this.in = in;
            this.cls1 = cls1;
            this.cls2 = cls2;
        }

        public Single<Tuple2<T1, T2>> build() {
            // TODO Auto-generated method stub
            return null;
        }
    }

    public static final class CallableResultSets1Builder<T1> {

        private final List<Object> in;
        private final Function<? super ResultSet, ? extends T1> f1;

        CallableResultSets1Builder(List<Object> in,
                Function<? super ResultSet, ? extends T1> function) {
            this.in = in;
            this.f1 = function;
        }

        public <T2> CallableResultSets2Builder<T1, T2> autoMap(Class<T2> cls) {
            return new CallableResultSets2Builder<T1, T2>(in, f1, Util.autoMap(cls));
        }
    }

    public static final class CallableResultSets2Builder<T1, T2> {

        private final List<Object> in;
        private final Function<? super ResultSet, ? extends T1> f1;
        private final Function<? super ResultSet, ? extends T2> f2;

        CallableResultSets2Builder(List<Object> in, Function<? super ResultSet, ? extends T1> f1,
                Function<? super ResultSet, ? extends T2> f2) {
            this.in = in;
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

    public static void main(String[] args) {
        {
            // in and out parameters are ordered by position in sql
            Single<Tuple2<Integer, String>> result = new CallableBuilder("call doit(?,?,?,?)") //
                    .in(10) //
                    .inOut(5, Integer.class) //
                    .out(String.class) //
                    .build();
        }
        {
            // result set returns don't have parameters and are at the end of the java
            // procedure declaration (can this position vary?)
            Single<CallableResultSet2<Person, Person>> result = new CallableBuilder("call doit(?)") //
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
