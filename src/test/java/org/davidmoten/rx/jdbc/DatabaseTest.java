package org.davidmoten.rx.jdbc;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.sql.Connection;
import java.sql.SQLSyntaxErrorException;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.davidmoten.rx.jdbc.annotations.Column;
import org.davidmoten.rx.jdbc.annotations.Index;
import org.davidmoten.rx.jdbc.annotations.Query;
import org.davidmoten.rx.jdbc.exceptions.AnnotationsNotFoundException;
import org.davidmoten.rx.jdbc.exceptions.ColumnIndexOutOfRangeException;
import org.davidmoten.rx.jdbc.exceptions.ColumnNotFoundException;
import org.davidmoten.rx.jdbc.exceptions.MoreColumnsRequestedThanExistException;
import org.davidmoten.rx.jdbc.exceptions.NamedParameterMissingException;
import org.davidmoten.rx.jdbc.exceptions.QueryAnnotationMissingException;
import org.davidmoten.rx.jdbc.pool.DatabaseCreator;
import org.davidmoten.rx.jdbc.pool.NonBlockingConnectionPool;
import org.davidmoten.rx.jdbc.pool.PoolClosedException;
import org.davidmoten.rx.jdbc.pool.Pools;
import org.davidmoten.rx.jdbc.tuple.Tuple2;
import org.davidmoten.rx.jdbc.tuple.Tuple3;
import org.davidmoten.rx.jdbc.tuple.Tuple4;
import org.davidmoten.rx.jdbc.tuple.Tuple5;
import org.davidmoten.rx.jdbc.tuple.Tuple6;
import org.davidmoten.rx.jdbc.tuple.Tuple7;
import org.davidmoten.rx.jdbc.tuple.TupleN;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.reactivex.Completable;
import io.reactivex.Flowable;
import io.reactivex.Observable;
import io.reactivex.Single;
import io.reactivex.functions.Predicate;
import io.reactivex.schedulers.Schedulers;
import io.reactivex.schedulers.TestScheduler;
import io.reactivex.subscribers.TestSubscriber;

public class DatabaseTest {

    private static final long FRED_REGISTERED_TIME = 1442515672690L;
    private static final int NAMES_COUNT_BIG = 5163;
    private static final Logger log = LoggerFactory.getLogger(DatabaseTest.class);

    private static Database db() {
        return DatabaseCreator.create(1);
    }

    private static Database db(int poolSize) {
        return DatabaseCreator.create(poolSize);
    }

    private static Database big(int poolSize) {
        return DatabaseCreator.create(poolSize, true);
    }

    @Test
    public void testSelectUsingQuestionMark() {
        db().select("select score from person where name=?") //
                .parameters("FRED", "JOSEPH") //
                .getAs(Integer.class) //
                .test() //
                .assertNoErrors() //
                .assertValues(21, 34) //
                .assertComplete();
    }

    @Test
    public void testSelectUsingQuestionMarkFlowableParameters() {
        db().select("select score from person where name=?") //
                .parameterStream(Flowable.just("FRED", "JOSEPH")) //
                .getAs(Integer.class) //
                .test() //
                .assertNoErrors() //
                .assertValues(21, 34) //
                .assertComplete();
    }

    @Test
    public void testSelectUsingQuestionMarkFlowableParametersInLists() {
        db().select("select score from person where name=?") //
                .parameterListStream(Flowable.just(Arrays.asList("FRED"), Arrays.asList("JOSEPH"))) //
                .getAs(Integer.class) //
                .test() //
                .assertNoErrors() //
                .assertValues(21, 34) //
                .assertComplete();
    }

    @Test
    public void testDrivingSelectWithoutParametersUsingParameterStream() {
        db().select("select count(*) from person") //
                .parameters(1, 2, 3) //
                .getAs(Integer.class) //
                .test() //
                .assertValues(3, 3, 3) //
                .assertComplete();
    }

    @Test
    public void testSelectUsingQuestionMarkFlowableParametersTwoParametersPerQuery() {
        db().select("select score from person where name=? and score = ?") //
                .parameterStream(Flowable.just("FRED", 21, "JOSEPH", 34)) //
                .getAs(Integer.class) //
                .test() //
                .assertNoErrors() //
                .assertValues(21, 34) //
                .assertComplete();
    }

    @Test
    public void testSelectUsingQuestionMarkFlowableParameterListsTwoParametersPerQuery() {
        db().select("select score from person where name=? and score = ?") //
                .parameterListStream(Flowable.just(Arrays.asList("FRED", 21), Arrays.asList("JOSEPH", 34))) //
                .getAs(Integer.class) //
                .test() //
                .assertNoErrors() //
                .assertValues(21, 34) //
                .assertComplete();
    }

    @Test
    public void testSelectUsingQuestionMarkWithPublicTestingDatabase() {
        Database.test() //
                .select("select score from person where name=?") //
                .parameters("FRED", "JOSEPH") //
                .getAs(Integer.class) //
                .test() //
                .assertNoErrors() //
                .assertValues(21, 34) //
                .assertComplete();
    }

    @Test
    public void testSelectUsingNonBlockingBuilder() {
        NonBlockingConnectionPool pool = Pools //
                .nonBlocking() //
                .connectionProvider(DatabaseCreator.connectionProvider()) //
                .maxIdleTime(1, TimeUnit.MINUTES) //
                .idleTimeBeforeHealthCheck(1, TimeUnit.MINUTES) //
                .healthy(c -> c.prepareStatement("select 1").execute()) //
                .returnToPoolDelayAfterHealthCheckFailure(1, TimeUnit.SECONDS) //
                .maxPoolSize(3) //
                .build();

        try (Database db = Database.from(pool)) {
            db.select("select score from person where name=?") //
                    .parameters("FRED", "JOSEPH") //
                    .getAs(Integer.class) //
                    .test() //
                    .assertNoErrors() //
                    .assertValues(21, 34) //
                    .assertComplete();
        }
    }

    @Test
    public void testDatabaseClose() {
        try (Database db = db()) {
            db.select("select score from person where name=?") //
                    .parameters("FRED", "JOSEPH") //
                    .getAs(Integer.class) //
                    .test() //
                    .assertNoErrors() //
                    .assertValues(21, 34) //
                    .assertComplete();
        }
    }

    @Test
    public void testSelectUsingName() {
        db() //
                .select("select score from person where name=:name") //
                .parameter("name", "FRED") //
                .parameter("name", "JOSEPH") //
                .getAs(Integer.class) //
                .test() //
                .assertValues(21, 34) //
                .assertComplete();
    }

    @Test
    public void testSelectUsingNameNotGiven() {
        db() //
                .select("select score from person where name=:name and name<>:name2") //
                .parameter("name", "FRED") //
                .parameter("name", "JOSEPH") //
                .getAs(Integer.class) //
                .test() //
                .assertError(NamedParameterMissingException.class).assertNoValues();
    }

    @Test(expected = NullPointerException.class)
    public void testSelectUsingNullNameInParameter() {
        db() //
                .select("select score from person where name=:name") //
                .parameter(null, "FRED"); //
    }

    @Test(expected = IllegalArgumentException.class)
    public void testSelectUsingNameDoesNotExist() {
        db() //
                .select("select score from person where name=:name") //
                .parameters("nam", "FRED");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testSelectUsingNameWithoutSpecifyingNameThrowsImmediately() {
        db() //
                .select("select score from person where name=:name") //
                .parameters("FRED", "JOSEPH");
    }

    @Test
    public void testSelectTransacted() {
        System.out.println("testSelectTransacted");
        db() //
                .select("select score from person where name=?") //
                .parameters("FRED", "JOSEPH") //
                .transacted() //
                .getAs(Integer.class) //
                .doOnNext(tx -> System.out.println(tx.isComplete() ? "complete" : tx.value())) //
                .test() //
                .assertValueCount(3) //
                .assertComplete();
    }

    @Test
    public void testSelectTransactedChained() throws Exception {
        Database db = db();
        db //
                .select("select score from person where name=?") //
                .parameters("FRED", "JOSEPH") //
                .transacted() //
                .transactedValuesOnly() //
                .getAs(Integer.class) //
                .doOnNext(tx -> System.out.println(tx.isComplete() ? "complete" : tx.value()))//
                .flatMap(tx -> tx //
                        .select("select name from person where score = ?") //
                        .parameters(tx.value()) //
                        .valuesOnly() //
                        .getAs(String.class)) //
                .test() //
                .assertNoErrors() //
                .assertValues("FRED", "JOSEPH") //
                .assertComplete();
    }

    @Test
    public void databaseIsAutoCloseable() {
        try (Database db = db()) {
            log.debug(db.toString());
        }
    }

    @Test
    public void testSelectChained() {
        System.out.println("testSelectChained");
        // we can do this with 1 connection only!
        Database db = db(1);
        db.select("select score from person where name=?") //
                .parameters("FRED", "JOSEPH") //
                .getAs(Integer.class) //
                .doOnNext(System.out::println) //
                .concatMap(score -> {
                    log.info("score={}", score);
                    return db //
                            .select("select name from person where score = ?") //
                            .parameters(score) //
                            .getAs(String.class) //
                            .doOnComplete(() -> log.info("completed select where score=" + score));
                }) //
                .test() //
                .assertNoErrors() //
                .assertValues("FRED", "JOSEPH") //
                .assertComplete(); //
    }

    @Test
    @SuppressFBWarnings
    public void testReadMeFragment1() {
        Database db = Database.test();
        db.select("select name from person") //
                .getAs(String.class) //
                .forEach(System.out::println);
    }

    @Test
    public void testReadMeFragmentColumnDoesNotExistEmitsSqlSyntaxErrorException() {
        Database db = Database.test();
        db.select("select nam from person") //
                .getAs(String.class) //
                .test() //
                .assertNoValues() //
                .assertError(SQLSyntaxErrorException.class);
    }

    @Test
    public void testReadMeFragmentDerbyHealthCheck() {
        Database db = Database.test();
        db.select("select 'a' from sysibm.sysdummy1") //
                .getAs(String.class) //
                .test() //
                .assertValue("a") //
                .assertComplete();
    }

    @Test
    @SuppressFBWarnings
    public void testTupleSupport() {
        db().select("select name, score from person") //
                .getAs(String.class, Integer.class) //
                .forEach(System.out::println);
    }

    @Test
    public void testDelayedCallsAreNonBlocking() throws InterruptedException {
        List<String> list = new CopyOnWriteArrayList<String>();
        Database db = db(1); //
        db.select("select score from person where name=?") //
                .parameters("FRED") //
                .getAs(Integer.class) //
                .doOnNext(x -> Thread.sleep(1000)) //
                .subscribeOn(Schedulers.io()) //
                .subscribe();
        Thread.sleep(100);
        CountDownLatch latch = new CountDownLatch(1);
        db.select("select score from person where name=?") //
                .parameters("FRED") //
                .getAs(Integer.class) //
                .doOnNext(x -> list.add("emitted")) //
                .doOnNext(x -> System.out.println("emitted on " + Thread.currentThread().getName())) //
                .doOnNext(x -> latch.countDown()) //
                .subscribe();
        list.add("subscribed");
        assertTrue(latch.await(5, TimeUnit.SECONDS));
        assertEquals(Arrays.asList("subscribed", "emitted"), list);
    }

    @Test
    public void testAutoMapToInterface() {
        db() //
                .select("select name from person") //
                .autoMap(Person.class) //
                .map(p -> p.name()) //
                .test() //
                .assertValueCount(3) //
                .assertComplete();
    }

    @Test
    public void testAutoMapToInterfaceWithoutAnnotationsEmitsError() {
        db() //
                .select("select name from person") //
                .autoMap(PersonNoAnnotation.class) //
                .map(p -> p.name()) //
                .test() //
                .assertNoValues() //
                .assertError(AnnotationsNotFoundException.class);
    }

    @Test
    public void testAutoMapToInterfaceWithTwoMethods() {
        db() //
                .select("select name, score from person order by name") //
                .autoMap(Person2.class) //
                .firstOrError() //
                .map(Person2::score) //
                .test() //
                .assertValue(21) //
                .assertComplete();
    }

    @Test
    public void testAutoMapToInterfaceWithExplicitColumnName() {
        db() //
                .select("select name, score from person order by name") //
                .autoMap(Person3.class) //
                .firstOrError() //
                .map(Person3::examScore) //
                .test() //
                .assertValue(21) //
                .assertComplete();
    }

    @Test
    public void testAutoMapToInterfaceWithExplicitColumnNameThatDoesNotExist() {
        db() //
                .select("select name, score from person order by name") //
                .autoMap(Person4.class) //
                .firstOrError() //
                .map(Person4::examScore) //
                .test() //
                .assertNoValues() //
                .assertError(ColumnNotFoundException.class);
    }

    @Test
    public void testAutoMapToInterfaceWithIndex() {
        db() //
                .select("select name, score from person order by name") //
                .autoMap(Person5.class) //
                .firstOrError() //
                .map(Person5::examScore) //
                .test() //
                .assertValue(21) //
                .assertComplete();
    }

    @Test
    public void testAutoMapToInterfaceWithIndexTooLarge() {
        db() //
                .select("select name, score from person order by name") //
                .autoMap(Person6.class) //
                .firstOrError() //
                .map(Person6::examScore) //
                .test() //
                .assertNoValues() //
                .assertError(ColumnIndexOutOfRangeException.class);
    }

    @Test
    public void testAutoMapToInterfaceWithIndexTooSmall() {
        db() //
                .select("select name, score from person order by name") //
                .autoMap(Person7.class) //
                .firstOrError() //
                .map(Person7::examScore) //
                .test() //
                .assertNoValues() //
                .assertError(ColumnIndexOutOfRangeException.class);
    }

    @Test
    public void testAutoMapWithUnmappableColumnType() {
        db() //
                .select("select name from person order by name") //
                .autoMap(Person8.class) //
                .map(p -> p.name()) //
                .test() //
                .assertNoValues() //
                .assertError(ClassCastException.class);
    }

    @Test
    public void testAutoMapWithMixIndexAndName() {
        db() //
                .select("select name, score from person order by name") //
                .autoMap(Person9.class) //
                .firstOrError() //
                .map(Person9::score) //
                .test() //
                .assertValue(21) //
                .assertComplete();
    }

    @Test
    public void testAutoMapWithQueryInAnnotation() {
        db().select(Person10.class) //
                .get() //
                .firstOrError() //
                .map(Person10::score) //
                .test() //
                .assertValue(21) //
                .assertComplete();
    }

    @Test(expected = QueryAnnotationMissingException.class)
    public void testAutoMapWithoutQueryInAnnotation() {
        db().select(Person.class);
    }

    @Test
    public void testSelectWithoutWhereClause() {
        Assert.assertEquals(3,
                (long) db().select("select name from person") //
                        .count() //
                        .blockingGet());
    }

    @Test
    public void testTuple3() {
        db() //
                .select("select name, score, name from person order by name") //
                .getAs(String.class, Integer.class, String.class) //
                .firstOrError() //
                .test() //
                .assertComplete() //
                .assertValue(Tuple3.create("FRED", 21, "FRED")); //
    }

    @Test
    public void testTuple4() {
        db() //
                .select("select name, score, name, score from person order by name") //
                .getAs(String.class, Integer.class, String.class, Integer.class) //
                .firstOrError() //
                .test() //
                .assertComplete() //
                .assertValue(Tuple4.create("FRED", 21, "FRED", 21)); //
    }

    @Test
    public void testTuple5() {
        db() //
                .select("select name, score, name, score, name from person order by name") //
                .getAs(String.class, Integer.class, String.class, Integer.class, String.class) //
                .firstOrError() //
                .test() //
                .assertComplete().assertValue(Tuple5.create("FRED", 21, "FRED", 21, "FRED")); //
    }

    @Test
    public void testTuple6() {
        db() //
                .select("select name, score, name, score, name, score from person order by name") //
                .getAs(String.class, Integer.class, String.class, Integer.class, String.class, Integer.class) //
                .firstOrError() //
                .test() //
                .assertComplete().assertValue(Tuple6.create("FRED", 21, "FRED", 21, "FRED", 21)); //
    }

    @Test
    public void testTuple7() {
        db() //
                .select("select name, score, name, score, name, score, name from person order by name") //
                .getAs(String.class, Integer.class, String.class, Integer.class, String.class, Integer.class,
                        String.class) //
                .firstOrError() //
                .test() //
                .assertComplete().assertValue(Tuple7.create("FRED", 21, "FRED", 21, "FRED", 21, "FRED")); //
    }

    @Test
    public void testTupleN() {
        db() //
                .select("select name, score, name from person order by name") //
                .getTupleN() //
                .firstOrError().test() //
                .assertComplete() //
                .assertValue(TupleN.create("FRED", 21, "FRED")); //
    }

    @Test
    public void testHealthCheck() throws InterruptedException {
        AtomicBoolean once = new AtomicBoolean(true);
        testHealthCheck(c -> {
            log.debug("doing health check");
            return !once.compareAndSet(true, false);
        });
    }

    @Test
    public void testHealthCheckThatThrows() throws InterruptedException {
        AtomicBoolean once = new AtomicBoolean(true);
        testHealthCheck(c -> {
            log.debug("doing health check");
            if (!once.compareAndSet(true, false))
                return true;
            else
                throw new RuntimeException("health check failed");
        });
    }

    @Test
    public void testUpdateOneRow() {
        db().update("update person set score=20 where name='FRED'") //
                .counts() //
                .test() //
                .assertValue(1) //
                .assertComplete();
    }

    @Test
    public void testUpdateThreeRows() {
        db().update("update person set score=20") //
                .counts() //
                .test() //
                .assertValue(3) //
                .assertComplete();
    }

    @Test
    public void testUpdateWithParameter() {
        db().update("update person set score=20 where name=?") //
                .parameters("FRED").counts() //
                .test() //
                .assertValue(1) //
                .assertComplete();
    }

    @Test
    public void testUpdateWithParameterTwoRuns() {
        db().update("update person set score=20 where name=?") //
                .parameters("FRED", "JOSEPH").counts() //
                .test() //
                .assertValues(1, 1) //
                .assertComplete();
    }

    @Test
    public void testUpdateAllWithParameterFourRuns() {
        db().update("update person set score=?") //
                .parameters(1, 2, 3, 4) //
                .counts() //
                .test() //
                .assertValues(3, 3, 3, 3) //
                .assertComplete();
    }

    @Test
    public void testUpdateWithBatchSize2() {
        db().update("update person set score=?") //
                .batchSize(2) //
                .parameters(1, 2, 3, 4) //
                .counts() //
                .test() //
                .assertValues(3, 3, 3, 3) //
                .assertComplete();
    }

    @Test
    public void testUpdateWithBatchSize3GreaterThanNumRecords() {
        db().update("update person set score=?") //
                .batchSize(3) //
                .parameters(1, 2, 3, 4) //
                .counts() //
                .test() //
                .assertValues(3, 3, 3, 3) //
                .assertComplete();
    }

    @Test
    public void testInsert() {
        Database db = db();
        db.update("insert into person(name, score) values(?,?)") //
                .parameters("DAVE", 12, "ANNE", 18) //
                .counts() //
                .test() //
                .assertValues(1, 1) //
                .assertComplete();
        List<Tuple2<String, Integer>> list = db.select("select name, score from person") //
                .getAs(String.class, Integer.class) //
                .toList() //
                .blockingGet();
        assertTrue(list.contains(Tuple2.create("DAVE", 12)));
        assertTrue(list.contains(Tuple2.create("ANNE", 18)));
    }

    @Test
    public void testReturnGeneratedKeys() {
        Database db = db();
        // note is a table with auto increment
        db.update("insert into note(text) values(?)") //
                .parameters("HI", "THERE") //
                .returnGeneratedKeys() //
                .getAs(Integer.class)//
                .test() //
                .assertValues(1, 2) //
                .assertComplete();

        db.update("insert into note(text) values(?)") //
                .parameters("ME", "TOO") //
                .returnGeneratedKeys() //
                .getAs(Integer.class)//
                .test() //
                .assertValues(3, 4) //
                .assertComplete();
    }

    @Test
    public void testReturnGeneratedKeysDerby() {
        Database db = DatabaseCreator.createDerby(1);

        // note is a table with auto increment
        db.update("insert into note2(text) values(?)") //
                .parameters("HI", "THERE") //
                .returnGeneratedKeys() //
                .getAs(Integer.class)//
                .test() //
                .assertNoErrors().assertValues(1, 3) //
                .assertComplete();

        db.update("insert into note2(text) values(?)") //
                .parameters("ME", "TOO") //
                .returnGeneratedKeys() //
                .getAs(Integer.class)//
                .test() //
                .assertValues(5, 7) //
                .assertComplete();
    }

    @Test
    public void testTransactedReturnGeneratedKeys() {
        Database db = db();
        // note is a table with auto increment
        db.update("insert into note(text) values(?)") //
                .parameters("HI", "THERE") //
                .transacted() //
                .returnGeneratedKeys() //
                .getAs(Integer.class)//
                .test() //
                .assertValues(1, 2) //
                .assertComplete();

        db.update("insert into note(text) values(?)") //
                .parameters("ME", "TOO") //
                .transacted() //
                .returnGeneratedKeys() //
                .getAs(Integer.class)//
                .test() //
                .assertValues(3, 4) //
                .assertComplete();
    }

    @Test
    public void testTransactedReturnGeneratedKeys2() {
        Database db = db();
        // note is a table with auto increment
        Flowable<Integer> a = db.update("insert into note(text) values(?)") //
                .parameters("HI", "THERE") //
                .transacted() //
                .returnGeneratedKeys() //
                .getAs(Integer.class);

        db.update("insert into note(text) values(?)") //
                .parameters("ME", "TOO") //
                .transacted() //
                .returnGeneratedKeys() //
                .getAs(Integer.class)//
                .startWith(a) //
                .test() //
                .assertValues(1, 2, 3, 4) //
                .assertComplete();
    }

    @Test
    public void testUpdateWithinTransaction() {
        db() //
                .select("select name from person") //
                .transactedValuesOnly() //
                .getAs(String.class) //
                .doOnNext(System.out::println) //
                .flatMap(tx -> tx//
                        .update("update person set score=-1 where name=:name") //
                        .batchSize(1) //
                        .parameter("name", tx.value()) //
                        .valuesOnly() //
                        .counts()) //
                .test() //
                .assertValues(1, 1, 1) //
                .assertComplete();
    }

    @Test
    public void testSelectDependsOnFlowable() {
        Database db = db();
        Flowable<Integer> a = db.update("update person set score=100 where name=?") //
                .parameters("FRED") //
                .counts();
        db.select("select score from person where name=?") //
                .parameters("FRED") //
                .dependsOn(a) //
                .getAs(Integer.class)//
                .test() //
                .assertValues(100) //
                .assertComplete();
    }

    @Test
    public void testSelectDependsOnObservable() {
        Database db = db();
        Observable<Integer> a = db.update("update person set score=100 where name=?") //
                .parameters("FRED") //
                .counts().toObservable();
        db.select("select score from person where name=?") //
                .parameters("FRED") //
                .dependsOn(a) //
                .getAs(Integer.class)//
                .test() //
                .assertValues(100) //
                .assertComplete();
    }

    @Test
    public void testSelectDependsOnOnSingle() {
        Database db = db();
        Single<Long> a = db.update("update person set score=100 where name=?") //
                .parameters("FRED") //
                .counts().count();
        db.select("select score from person where name=?") //
                .parameters("FRED") //
                .dependsOn(a) //
                .getAs(Integer.class)//
                .test() //
                .assertValues(100) //
                .assertComplete();
    }

    @Test
    public void testSelectDependsOnCompletable() {
        Database db = db();
        Completable a = db.update("update person set score=100 where name=?") //
                .parameters("FRED") //
                .counts().ignoreElements();
        db.select("select score from person where name=?") //
                .parameters("FRED") //
                .dependsOn(a) //
                .getAs(Integer.class)//
                .test() //
                .assertValues(100) //
                .assertComplete();
    }

    @Test
    public void testUpdateWithinTransactionBatchSize0() {
        db() //
                .select("select name from person") //
                .transactedValuesOnly() //
                .getAs(String.class) //
                .doOnNext(System.out::println) //
                .flatMap(tx -> tx//
                        .update("update person set score=-1 where name=:name") //
                        .batchSize(0) //
                        .parameter("name", tx.value()) //
                        .valuesOnly() //
                        .counts()) //
                .test() //
                .assertValues(1, 1, 1) //
                .assertComplete();
    }

    private static void info() {
        LogManager.getRootLogger().setLevel(Level.INFO);
    }

    private static void debug() {
        LogManager.getRootLogger().setLevel(Level.DEBUG);
    }

    @Test
    public void testCreateBig() {
        info();
        big(5).select("select count(*) from person") //
                .getAs(Integer.class) //
                .test() //
                .assertValue(5163) //
                .assertComplete();
        debug();
    }

    @Test
    public void testTxWithBig() {
        info();
        big(1) //
                .select("select name from person") //
                .transactedValuesOnly() //
                .getAs(String.class) //
                .flatMap(tx -> tx//
                        .update("update person set score=-1 where name=:name") //
                        .batchSize(1) //
                        .parameter("name", tx.value()) //
                        .valuesOnly() //
                        .counts()) //
                .count() //
                .test() //
                .assertValue((long) NAMES_COUNT_BIG) //
                .assertComplete();
        debug();
    }

    @Test
    public void testTxWithBigInputBatchSize2000() {
        info();
        big(1) //
                .select("select name from person") //
                .transactedValuesOnly() //
                .getAs(String.class) //
                .flatMap(tx -> tx//
                        .update("update person set score=-1 where name=:name") //
                        .batchSize(2000) //
                        .parameter("name", tx.value()) //
                        .valuesOnly() //
                        .counts()) //
                .count() //
                .test() //
                .assertValue((long) NAMES_COUNT_BIG) //
                .assertComplete();
        debug();
    }

    @Test
    public void testInsertClobAndReadClobAsString() {
        Database db = db();
        db.update("insert into person_clob(name,document) values(?,?)") //
                .parameters("FRED", "some text here") //
                .counts() //
                .test() //
                .assertValue(1) //
                .assertComplete();
        db.select("select document from person_clob where name='FRED'") //
                .getAs(String.class) //
                .test() //
                .assertValue("some text here") //
                .assertComplete();
    }

    @Test
    public void testInsertClobAndReadClobUsingReader() {
        Database db = db();
        db.update("insert into person_clob(name,document) values(?,?)") //
                .parameters("FRED", "some text here") //
                .counts() //
                .test() //
                .assertValue(1) //
                .assertComplete();
        db.select("select document from person_clob where name='FRED'") //
                .getAs(Reader.class) //
                .map(r -> read(r)).test() //
                .assertValue("some text here") //
                .assertComplete();
    }

    @Test
    public void testInsertBlobAndReadBlobAsByteArray() {
        Database db = db();
        byte[] bytes = "some text here".getBytes();
        db.update("insert into person_blob(name,document) values(?,?)") //
                .parameters("FRED", bytes) //
                .counts() //
                .test() //
                .assertValue(1) //
                .assertComplete();
        db.select("select document from person_blob where name='FRED'") //
                .getAs(byte[].class) //
                .map(b -> new String(b)) //
                .test() //
                .assertValue("some text here") //
                .assertComplete();
    }

    @Test
    public void testInsertBlobAndReadBlobAsInputStream() {
        Database db = db();
        byte[] bytes = "some text here".getBytes();
        db.update("insert into person_blob(name,document) values(?,?)") //
                .parameters("FRED", new ByteArrayInputStream(bytes)) //
                .counts() //
                .test() //
                .assertValue(1) //
                .assertComplete();
        db.select("select document from person_blob where name='FRED'") //
                .getAs(InputStream.class) //
                .map(is -> read(is)) //
                .map(b -> new String(b)) //
                .test() //
                .assertValue("some text here") //
                .assertComplete();
    }

    private static String read(Reader reader) throws IOException {
        StringBuffer s = new StringBuffer();
        char[] ch = new char[128];
        int n = 0;
        while ((n = reader.read(ch)) != -1) {
            s.append(ch, 0, n);
        }
        reader.close();
        return s.toString();
    }

    private static byte[] read(InputStream is) throws IOException {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        byte[] b = new byte[128];
        int n = 0;
        while ((n = is.read(b)) != -1) {
            bytes.write(b, 0, n);
        }
        is.close();
        return bytes.toByteArray();
    }

    private void testHealthCheck(Predicate<Connection> healthy) throws InterruptedException {
        TestScheduler scheduler = new TestScheduler();

        NonBlockingConnectionPool pool = Pools //
                .nonBlocking() //
                .connectionProvider(DatabaseCreator.connectionProvider()) //
                .maxIdleTime(10, TimeUnit.MINUTES) //
                .idleTimeBeforeHealthCheck(0, TimeUnit.MINUTES) //
                .healthy(healthy) //
                .returnToPoolDelayAfterHealthCheckFailure(1, TimeUnit.MINUTES) //
                .scheduler(scheduler) //
                .maxPoolSize(1) //
                .build();

        try (Database db = Database.from(pool)) {
            db.select( //
                    "select score from person where name=?") //
                    .parameters("FRED") //
                    .getAs(Integer.class) //
                    .test() //
                    .assertValueCount(1) //
                    .assertComplete();
            TestSubscriber<Integer> ts = db
                    .select( //
                            "select score from person where name=?") //
                    .parameters("FRED") //
                    .getAs(Integer.class) //
                    .test() //
                    .assertValueCount(0);
            scheduler.advanceTimeBy(1, TimeUnit.MINUTES);
            Thread.sleep(200);
            ts.assertValueCount(1);
            Thread.sleep(200);
            ts.assertValue(21) //
                    .assertComplete();
        }
    }

    @Test
    public void testShutdownBeforeUse() {
        NonBlockingConnectionPool pool = Pools //
                .nonBlocking() //
                .connectionProvider(DatabaseCreator.connectionProvider()) //
                .scheduler(Schedulers.io()) //
                .maxPoolSize(1) //
                .build();
        pool.close();
        Database.from(pool) //
                .select("select score from person where name=?") //
                .parameters("FRED") //
                .getAs(Integer.class) //
                .test() //
                .assertNoValues() //
                .assertError(PoolClosedException.class);
    }

    @Test
    public void testFewerColumnsMappedThanAvailable() {
        db().select("select name, score from person where name='FRED'") //
                .getAs(String.class) //
                .test() //
                .assertValues("FRED") //
                .assertComplete();
    }

    @Test
    public void testMoreColumnsMappedThanAvailable() {
        db() //
                .select("select name, score from person where name='FRED'") //
                .getAs(String.class, Integer.class, String.class) //
                .test() //
                .assertNoValues() //
                .assertError(MoreColumnsRequestedThanExistException.class);
    }

    @Test
    public void testSelectTimestamp() {
        db() //
                .select("select registered from person where name='FRED'") //
                .getAs(Long.class) //
                .test() //
                .assertValue(FRED_REGISTERED_TIME) //
                .assertComplete();
    }

    @Test
    public void testSelectTimestampAsDate() {
        db() //
                .select("select registered from person where name='FRED'") //
                .getAs(Date.class) //
                .map(d -> d.getTime()) //
                .test() //
                .assertValue(FRED_REGISTERED_TIME) //
                .assertComplete();
    }

    @Test
    public void testSelectTimestampAsInstant() {
        db() //
                .select("select registered from person where name='FRED'") //
                .getAs(Instant.class) //
                .map(d -> d.toEpochMilli()) //
                .test() //
                .assertValue(FRED_REGISTERED_TIME) //
                .assertComplete();
    }

    @Test
    public void testUpdateTimestampAsInstant() {
        Database db = db();
        db.update("update person set registered=? where name='FRED'") //
                .parameters(Instant.ofEpochMilli(FRED_REGISTERED_TIME)) //
                .counts() //
                .test() //
                .assertValue(1) //
                .assertComplete();
        db.select("select registered from person where name='FRED'") //
                .getAs(Long.class) //
                .test() //
                .assertValue(FRED_REGISTERED_TIME) //
                .assertComplete();
    }

    @Test
    public void testUpdateTimestampAsZonedDateTime() {
        Database db = db();
        db.update("update person set registered=? where name='FRED'") //
                .parameters(ZonedDateTime.ofInstant(Instant.ofEpochMilli(FRED_REGISTERED_TIME),
                        ZoneOffset.UTC.normalized())) //
                .counts() //
                .test() //
                .assertValue(1) //
                .assertComplete();
        db.select("select registered from person where name='FRED'") //
                .getAs(Long.class) //
                .test() //
                .assertValue(FRED_REGISTERED_TIME) //
                .assertComplete();
    }

    @Test
    public void testComplete() throws InterruptedException {
        Database db = db(1);
        Completable a = db //
                .update("update person set score=-3 where name='FRED'") //
                .complete();
        db.update("update person set score=-4 where score = -3") //
                .dependsOn(a) //
                .counts() //
                .test() //
                .assertValue(1) //
                .assertComplete();
    }

    @Test
    public void testCountsOnlyInTransaction() {
        db().update("update person set score = -3") //
                .transacted() //
                .countsOnly() //
                .test() //
                .assertValues(3) //
                .assertComplete();
    }

    @Test
    public void testCountsInTransaction() {
        db().update("update person set score = -3") //
                .transacted() //
                .counts() //
                .doOnNext(System.out::println) //
                .toList() //
                .test() //
                .assertValue(list -> list.get(0).isValue() && list.get(0).value() == 3 && list.get(1).isComplete()
                        && list.size() == 2) //
                .assertComplete();
    }

    @Test
    public void testTx() throws InterruptedException {
        Database db = db(3);
        Single<Tx<?>> transaction = db //
                .update("update person set score=-3 where name='FRED'") //
                .transaction();

        transaction //
                .doOnDispose(() -> System.out.println("disposing")) //
                .doOnSuccess(System.out::println) //
                .flatMapPublisher(tx -> {
                    System.out.println("flatmapping");
                    return tx //
                            .update("update person set score = -4 where score = -3") //
                            .countsOnly() //
                            .doOnSubscribe(s -> System.out.println("subscribed")) //
                            .doOnNext(num -> System.out.println("num=" + num));
                }) //
                .test() //
                .assertNoErrors() //
                .assertValue(1) //
                .assertComplete();
    }

    @Test
    public void testTxAfterSelect() {
        Database db = db(3);
        Single<Tx<Integer>> transaction = db //
                .select("select score from person where name='FRED'") //
                .transactedValuesOnly() //
                .getAs(Integer.class) //
                .firstOrError();

        transaction //
                .doOnDispose(() -> System.out.println("disposing")) //
                .doOnSuccess(System.out::println) //
                .flatMapPublisher(tx -> {
                    System.out.println("flatmapping");
                    return tx //
                            .update("update person set score = -4 where score = ?") //
                            .parameters(tx.value()) //
                            .countsOnly() //
                            .doOnSubscribe(s -> System.out.println("subscribed")) //
                            .doOnNext(num -> System.out.println("num=" + num));
                }) //
                .test() //
                .assertNoErrors() //
                .assertValue(1) //
                .assertComplete();
    }

    @Test
    public void testSingleFlatMap() {
        Single.just(1).flatMapPublisher(n -> Flowable.just(1)).test(1).assertValue(1).assertComplete();
    }

    interface Person {
        @Column
        String name();
    }

    interface Person2 {
        @Column
        String name();

        @Column
        int score();
    }

    interface Person3 {
        @Column("name")
        String fullName();

        @Column("score")
        int examScore();
    }

    interface Person4 {
        @Column("namez")
        String fullName();

        @Column("score")
        int examScore();
    }

    interface Person5 {
        @Index(1)
        String fullName();

        @Index(2)
        int examScore();
    }

    interface Person6 {
        @Index(1)
        String fullName();

        @Index(3)
        int examScore();
    }

    interface Person7 {
        @Index(1)
        String fullName();

        @Index(0)
        int examScore();
    }

    interface Person8 {
        @Column
        int name();
    }

    interface Person9 {
        @Column
        String name();

        @Index(2)
        int score();
    }

    interface PersonNoAnnotation {
        String name();
    }

    @Query("select name, score from person order by name")
    interface Person10 {

        @Column
        String name();

        @Column
        int score();
    }

}
