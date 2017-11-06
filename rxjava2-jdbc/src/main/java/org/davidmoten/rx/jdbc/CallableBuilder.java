package org.davidmoten.rx.jdbc;

import java.sql.ResultSet;

import io.reactivex.Completable;
import io.reactivex.functions.Function;

public class CallableBuilder {

    private final String sql;

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

    public <T> CallableBuilder1<T> out(Class<T> cls) {
        return new CallableBuilder1<T>(cls);
    }

    public <T> CallableResultSets1<T> map(Class<T> cls) {
        return new CallableResultSets1<T>(Util.autoMap(cls));
    }

    public static final class CallableBuilder1<T1> {

        private final Class<T1> cls;

        public CallableBuilder1(Class<T1> cls) {
            this.cls = cls;
        }

        public <T2> CallableBuilder2<T1, T2> out(Class<T2> cls2) {
            return new CallableBuilder2<T1, T2>(cls, cls2);
        }
    }

    public static final class CallableBuilder2<T1, T2> {

        private final Class<T1> cls1;
        private final Class<T2> cls2;

        public CallableBuilder2(Class<T1> cls1, Class<T2> cls2) {
            this.cls1 = cls1;
            this.cls2 = cls2;
        }
    }

    public static final class CallableResultSets1<T1> {

        private final Function<? super ResultSet, ? extends T1> f1;

        CallableResultSets1(Function<? super ResultSet, ? extends T1> function) {
            this.f1 = function;
        }

        public <T2> CallableResultSets2<T1, T2> map(Class<T2> cls) {
            return new CallableResultSets2<T1, T2>(f1, Util.autoMap(cls));
        }
    }

    public static final class CallableResultSets2<T1, T2> {

        private final Function<? super ResultSet, ? extends T1> f1;
        private final Function<? super ResultSet, ? extends T2> f2;

        CallableResultSets2(Function<? super ResultSet, ? extends T1> f1,
                Function<? super ResultSet, ? extends T2> f2) {
            this.f1 = f1;
            this.f2 = f2;
        }

        public <T3> CallableResultSets3<T1, T2, T3> map(Class<T3> cls) {
            return new CallableResultSets3<T1, T2, T3>(f1, f2, Util.autoMap(cls));
        }
    }

    public static final class CallableResultSets3<T1, T2, T3> {

        private final Function<? super ResultSet, ? extends T1> f1;
        private final Function<? super ResultSet, ? extends T2> f2;
        private final Function<? super ResultSet, ? extends T3> f3;

        CallableResultSets3(Function<? super ResultSet, ? extends T1> f1,
                Function<? super ResultSet, ? extends T2> f2,
                Function<? super ResultSet, ? extends T3> f3) {
            this.f1 = f1;
            this.f2 = f2;
            this.f3 = f3;
        }

    }

}
