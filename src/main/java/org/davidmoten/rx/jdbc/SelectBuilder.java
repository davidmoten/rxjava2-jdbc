package org.davidmoten.rx.jdbc;

import java.sql.Connection;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.davidmoten.rx.jdbc.tuple.Tuple2;
import org.davidmoten.rx.jdbc.tuple.Tuple3;
import org.davidmoten.rx.jdbc.tuple.Tuple4;
import org.davidmoten.rx.jdbc.tuple.Tuple5;
import org.davidmoten.rx.jdbc.tuple.Tuple6;
import org.davidmoten.rx.jdbc.tuple.Tuple7;
import org.davidmoten.rx.jdbc.tuple.TupleN;
import org.davidmoten.rx.jdbc.tuple.Tuples;

import com.github.davidmoten.guavamini.Preconditions;

import io.reactivex.Flowable;
import io.reactivex.Single;

public class SelectBuilder {

    String sql;
    private final SqlInfo sqlInfo;
    final Flowable<Connection> connections;

    // mutable
    final List<Flowable<List<Object>>> parameterGroups = new ArrayList<>();
    final List<Object> parameters = new ArrayList<>();
    boolean valuesOnly = false;
    int fetchSize = 0; // default

    public SelectBuilder(String sql, Flowable<Connection> connections) {
        Preconditions.checkNotNull(sql);
        Preconditions.checkNotNull(connections);
        this.sql = sql;
        this.connections = connections;
        this.sqlInfo = SqlInfo.parse(sql);
    }

    public SelectBuilder parameters(Flowable<List<Object>> parameters) {
        closeCurrentParameterList();
        Preconditions.checkNotNull(parameters);
        this.parameterGroups.add(parameters);
        return this;
    }

    @SuppressWarnings("unchecked")
    private void closeCurrentParameterList() {
        if (!parameters.isEmpty()) {
            parameterGroups.add(Flowable.just((List<Object>) (List<?>) parameters));
            parameters.clear();
        }
    }

    public SelectBuilder parameterList(List<Object> values) {
        closeCurrentParameterList();
        Preconditions.checkNotNull(values);
        this.parameterGroups.add(Flowable.just(values));
        return this;
    }

    public SelectBuilder parameterList(Object... values) {
        Preconditions.checkNotNull(values);
        closeCurrentParameterList();
        parameterGroups.add(Flowable.fromArray(values).buffer(sqlInfo.numParameters()));
        return this;
    }

    public SelectBuilder parameter(String name, Object value) {
        Preconditions.checkNotNull(name);
        parameters.add(new Parameter(name, value));
        return this;
    }

    public SelectBuilder fetchSize(int size) {
        this.fetchSize = size;
        return this;
    }

    public SelectBuilder parameters(Object... values) {
        Preconditions.checkNotNull(values);
        if (values.length == 0) {
            // no effect
            return this;
        }
        Preconditions.checkArgument(sqlInfo.numParameters() > 0, "no parameters present in sql!");
        Preconditions.checkArgument(values.length % sqlInfo.numParameters() == 0,
                "number of values should be a multiple of number of parameters in sql: " + sql);
        Preconditions.checkArgument(Arrays.stream(values).allMatch(o -> sqlInfo.names().isEmpty()
                || (o instanceof Parameter && ((Parameter) o).hasName())));
        for (Object val : values) {
            parameters.add(val);
        }
        return parameters(Flowable.fromArray(values).buffer(sqlInfo.numParameters()));
    }

    /**
     * Automaps the first column of the ResultSet into the target class
     * <code>cls</code>.
     * 
     * @param cls
     * @return
     */
    public <T> Flowable<T> getAs(Class<T> cls) {
        resolveParameters();
        // TODO make static class so lambda doesn't enclose more state than
        // should
        return get(rs -> Util.mapObject(rs, cls, 1));
    }

    void resolveParameters() {
        closeCurrentParameterList();
    }

    private static final Flowable<List<Object>> SINGLE_EMPTY_LIST = Flowable
            .just(Collections.emptyList());

    public TransactedSelectBuilder transacted() {
        return new TransactedSelectBuilder(this);
    }

    /**
     * Transforms the results using the given function.
     *
     * @param function
     * @return the results of the query as an Observable
     */
    public <T> Flowable<T> get(ResultSetMapper<? extends T> function) {
        Flowable<List<Object>> pg = programGroupsToFlowable();
        return Select.<T> create(connections.firstOrError(), pg, sql, fetchSize, function);
    }

    Flowable<List<Object>> programGroupsToFlowable() {
        resolveParameters();
        Flowable<List<Object>> pg;
        if (parameterGroups.isEmpty()) {
            pg = SINGLE_EMPTY_LIST;
        } else {
            pg = Flowable.concat(parameterGroups);
        }
        return pg;
    }

    //
    // static <T> Observable<T> get(ResultSetMapper<? extends T> function,
    // QueryBuilder builder,
    // Func1<ResultSet, ? extends ResultSet> resultSetTransform) {
    // return new QuerySelect(builder.sql(), builder.parameters(),
    // builder.depends(),
    // builder.context(), resultSetTransform).execute(function);
    // }
    //
    /**
     * <p>
     * Transforms each row of the {@link ResultSet} into an instance of
     * <code>T</code> using <i>automapping</i> of the ResultSet columns into
     * corresponding constructor parameters that are assignable. Beyond normal
     * assignable criteria (for example Integer 123 is assignable to a Double)
     * other conversions exist to facilitate the automapping:
     * </p>
     * <p>
     * They are:
     * <ul>
     * <li>java.sql.Blob &#10143; byte[]</li>
     * <li>java.sql.Blob &#10143; java.io.InputStream</li>
     * <li>java.sql.Clob &#10143; String</li>s
     * <li>java.sql.Clob &#10143; java.io.Reader</li>
     * <li>java.sql.Date &#10143; java.util.Date</li>
     * <li>java.sql.Date &#10143; Long</li>
     * <li>java.sql.Timestamp &#10143; java.util.Date</li>
     * <li>java.sql.Timestamp &#10143; Long</li>
     * <li>java.sql.Time &#10143; java.util.Date</li>
     * <li>java.sql.Time &#10143; Long</li>
     * <li>java.math.BigInteger &#10143;
     * Short,Integer,Long,Float,Double,BigDecimal</li>
     * <li>java.math.BigDecimal &#10143;
     * Short,Integer,Long,Float,Double,BigInteger</li>
     * </p>
     *
     * @param cls
     * @return
     */
    public <T> Flowable<T> autoMap(Class<T> cls) {
        if (sql == null) {
            sql = Util.getSqlFromQueryAnnotation(cls);
        }
        return get(Util.autoMap(cls));
    }

    /**
     * Automaps all the columns of the {@link ResultSet} into the target class
     * <code>cls</code>. See {@link #autoMap(Class) autoMap()}.
     *
     * @param cls
     * @return
     */
    public <S> Flowable<TupleN<S>> getTupleN(Class<S> cls) {
        return get(Tuples.tupleN(cls));
    }

    /**
     * Automaps all the columns of the {@link ResultSet} into {@link Object} .
     * See {@link #autoMap(Class) autoMap()}.
     *
     * @param cls
     * @return
     */
    public <S> Flowable<TupleN<Object>> getTupleN() {
        return get(Tuples.tupleN(Object.class));
    }

    /**
     * Automaps the columns of the {@link ResultSet} into the specified classes.
     * See {@link #autoMap(Class) autoMap()}.
     *
     * @param cls1
     * @param cls2
     * @return
     */
    public <T1, T2> Flowable<Tuple2<T1, T2>> getAs(Class<T1> cls1, Class<T2> cls2) {
        return get(Tuples.tuple(cls1, cls2));
    }

    /**
     * Automaps the columns of the {@link ResultSet} into the specified classes.
     * See {@link #autoMap(Class) autoMap()}.
     *
     * @param cls1
     * @param cls2
     * @param cls3
     * @return
     */
    public <T1, T2, T3> Flowable<Tuple3<T1, T2, T3>> getAs(Class<T1> cls1, Class<T2> cls2,
            Class<T3> cls3) {
        return get(Tuples.tuple(cls1, cls2, cls3));
    }

    /**
     * Automaps the columns of the {@link ResultSet} into the specified classes.
     * See {@link #autoMap(Class) autoMap()}.
     *
     * @param cls1
     * @param cls2
     * @param cls3
     * @param cls4
     * @return
     */
    public <T1, T2, T3, T4> Flowable<Tuple4<T1, T2, T3, T4>> getAs(Class<T1> cls1, Class<T2> cls2,
            Class<T3> cls3, Class<T4> cls4) {
        return get(Tuples.tuple(cls1, cls2, cls3, cls4));
    }

    /**
     * Automaps the columns of the {@link ResultSet} into the specified classes.
     * See {@link #autoMap(Class) autoMap()}.
     *
     * @param cls1
     * @param cls2
     * @param cls3
     * @param cls4
     * @param cls5
     * @return
     */
    public <T1, T2, T3, T4, T5> Flowable<Tuple5<T1, T2, T3, T4, T5>> getAs(Class<T1> cls1,
            Class<T2> cls2, Class<T3> cls3, Class<T4> cls4, Class<T5> cls5) {
        return get(Tuples.tuple(cls1, cls2, cls3, cls4, cls5));
    }

    /**
     * Automaps the columns of the {@link ResultSet} into the specified classes.
     * See {@link #autoMap(Class) autoMap()}.
     *
     * @param cls1
     * @param cls2
     * @param cls3
     * @param cls4
     * @param cls5
     * @param cls6
     * @return
     */
    public <T1, T2, T3, T4, T5, T6> Flowable<Tuple6<T1, T2, T3, T4, T5, T6>> getAs(Class<T1> cls1,
            Class<T2> cls2, Class<T3> cls3, Class<T4> cls4, Class<T5> cls5, Class<T6> cls6) {
        return get(Tuples.tuple(cls1, cls2, cls3, cls4, cls5, cls6));
    }

    /**
     * Automaps the columns of the {@link ResultSet} into the specified classes.
     * See {@link #autoMap(Class) autoMap()}.
     *
     * @param cls1
     * @param cls2
     * @param cls3
     * @param cls4
     * @param cls5
     * @param cls6
     * @param cls7
     * @return
     */
    public <T1, T2, T3, T4, T5, T6, T7> Flowable<Tuple7<T1, T2, T3, T4, T5, T6, T7>> getAs(
            Class<T1> cls1, Class<T2> cls2, Class<T3> cls3, Class<T4> cls4, Class<T5> cls5,
            Class<T6> cls6, Class<T7> cls7) {
        return get(Tuples.tuple(cls1, cls2, cls3, cls4, cls5, cls6, cls7));
    }

    public Single<Long> count() {
        return get(rs -> 1).count();
    }

    //
    // /**
    // * Returns an {@link Transformer} to allow the query to be pushed
    // * parameters via the {@link Observable#compose(Transformer)} method.
    // *
    // * @return Transformer that acts on parameters
    // */
    // public TransformerBuilder<Object> parameterTransformer() {
    // return new TransformerBuilder<Object>(this, OperatorType.PARAMETER);
    // }
    //
    // /**
    // * Returns an {@link Transformer} to allow the query to be pushed
    // * dependencies via the {@link Observable#compose(Transformer)} method.
    // *
    // * @return Transformer that acts on dependencies
    // */
    // public TransformerBuilder<Object> dependsOnTransformer() {
    // return new TransformerBuilder<Object>(this, OperatorType.DEPENDENCY);
    // }
    //
    // /**
    // * Returns an {@link Transformer} that runs a select query for each list
    // * of parameter objects in the source observable.
    // *
    // * @return
    // */
    // public TransformerBuilder<Observable<Object>> parameterListTransformer()
    // {
    // return new TransformerBuilder<Observable<Object>>(this,
    // OperatorType.PARAMETER_LIST);
    // }
    //
    //
    //

}
