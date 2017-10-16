package org.davidmoten.rx.jdbc;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLSyntaxErrorException;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.davidmoten.rx.jdbc.annotations.Column;
import org.davidmoten.rx.jdbc.annotations.Index;
import org.davidmoten.rx.jdbc.annotations.Query;
import org.davidmoten.rx.jdbc.exceptions.AnnotationsNotFoundException;
import org.davidmoten.rx.jdbc.exceptions.AutomappedInterfaceInaccessibleException;
import org.davidmoten.rx.jdbc.exceptions.ColumnIndexOutOfRangeException;
import org.davidmoten.rx.jdbc.exceptions.ColumnNotFoundException;
import org.davidmoten.rx.jdbc.exceptions.MoreColumnsRequestedThanExistException;
import org.davidmoten.rx.jdbc.exceptions.NamedParameterFoundButSqlDoesNotHaveNamesException;
import org.davidmoten.rx.jdbc.exceptions.NamedParameterMissingException;
import org.davidmoten.rx.jdbc.exceptions.QueryAnnotationMissingException;
import org.davidmoten.rx.jdbc.pool.DatabaseCreator;
import org.davidmoten.rx.jdbc.pool.NonBlockingConnectionPool;
import org.davidmoten.rx.jdbc.pool.Pools;
import org.davidmoten.rx.jdbc.tuple.Tuple2;
import org.davidmoten.rx.jdbc.tuple.Tuple3;
import org.davidmoten.rx.jdbc.tuple.Tuple4;
import org.davidmoten.rx.jdbc.tuple.Tuple5;
import org.davidmoten.rx.jdbc.tuple.Tuple6;
import org.davidmoten.rx.jdbc.tuple.Tuple7;
import org.davidmoten.rx.jdbc.tuple.TupleN;
import org.davidmoten.rx.pool.PoolClosedException;
import org.h2.jdbc.JdbcSQLException;
import org.hsqldb.jdbc.JDBCBlobFile;
import org.hsqldb.jdbc.JDBCClobFile;
import org.junit.Assert;
import org.junit.FixMethodOrder;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runners.MethodSorters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.davidmoten.guavamini.Lists;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.reactivex.Completable;
import io.reactivex.Flowable;
import io.reactivex.Observable;
import io.reactivex.Scheduler;
import io.reactivex.Single;
import io.reactivex.functions.Predicate;
import io.reactivex.schedulers.Schedulers;
import io.reactivex.schedulers.TestScheduler;
import io.reactivex.subscribers.TestSubscriber;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class DatabaseTest {

    private static final long FRED_REGISTERED_TIME = 1442515672690L;
    private static final int NAMES_COUNT_BIG = 5163;
    private static final Logger log = LoggerFactory.getLogger(DatabaseTest.class);
    private static final int TIMEOUT_SECONDS = 2;

    private static Database db() {
        return DatabaseCreator.create(1);
    }

    private static Database blocking() {
        return DatabaseCreator.createBlocking();
    }

    private static Database db(int poolSize) {
        return DatabaseCreator.create(poolSize);
    }

    private static Database big(int poolSize) {
        return DatabaseCreator.create(poolSize, true, Schedulers.computation());
    }

    @Test
    public void testSelectUsingQuestionMark() {
        try (Database db = db()) {
            db.select("select score from person where name=?") //
                    .parameters("FRED", "JOSEPH") //
                    .getAs(Integer.class) //
                    .test() //
                    .awaitDone(TIMEOUT_SECONDS, TimeUnit.SECONDS) //
                    .assertNoErrors() //
                    .assertValues(21, 34) //
                    .assertComplete();
        }
    }

    @Test
    public void testSelectUsingNamedParameterList() {
        try (Database db = db()) {
            db.select("select score from person where name=:name") //
                    .parameters(Parameter.named("name", "FRED").value("JOSEPH").list()) //
                    .getAs(Integer.class) //
                    .test() //
                    .awaitDone(TIMEOUT_SECONDS, TimeUnit.SECONDS) //
                    .assertNoErrors() //
                    .assertValues(21, 34) //
                    .assertComplete();
        }
    }

    @Test
    public void testSelectUsingQuestionMarkFlowableParameters() {
        try (Database db = db()) {
            db.select("select score from person where name=?") //
                    .parameterStream(Flowable.just("FRED", "JOSEPH")) //
                    .getAs(Integer.class) //
                    .test().awaitDone(TIMEOUT_SECONDS, TimeUnit.SECONDS) //
                    .awaitDone(TIMEOUT_SECONDS, TimeUnit.SECONDS) //
                    .assertNoErrors() //
                    .assertValues(21, 34) //
                    .assertComplete();
        }
    }

    @Test
    public void testSelectUsingQuestionMarkFlowableParametersInLists() {
        try (Database db = db()) {
            db.select("select score from person where name=?") //
                    .parameterListStream(
                            Flowable.just(Arrays.asList("FRED"), Arrays.asList("JOSEPH"))) //
                    .getAs(Integer.class) //
                    .test().awaitDone(TIMEOUT_SECONDS, TimeUnit.SECONDS) //
                    .awaitDone(TIMEOUT_SECONDS, TimeUnit.SECONDS) //
                    .assertNoErrors() //
                    .assertValues(21, 34) //
                    .assertComplete();
        }
    }

    @Test
    public void testDrivingSelectWithoutParametersUsingParameterStream() {
        try (Database db = db()) {
            db.select("select count(*) from person") //
                    .parameters(1, 2, 3) //
                    .getAs(Integer.class) //
                    .test().awaitDone(TIMEOUT_SECONDS, TimeUnit.SECONDS) //
                    .awaitDone(TIMEOUT_SECONDS, TimeUnit.SECONDS) //
                    .assertValues(3, 3, 3) //
                    .assertComplete();
        }
    }

    @Test
    public void testSelectUsingQuestionMarkFlowableParametersTwoParametersPerQuery() {
        try (Database db = db()) {
            db.select("select score from person where name=? and score = ?") //
                    .parameterStream(Flowable.just("FRED", 21, "JOSEPH", 34)) //
                    .getAs(Integer.class) //
                    .test().awaitDone(TIMEOUT_SECONDS, TimeUnit.SECONDS) //
                    .awaitDone(TIMEOUT_SECONDS, TimeUnit.SECONDS) //
                    .assertNoErrors() //
                    .assertValues(21, 34) //
                    .assertComplete();
        }
    }

    @Test
    public void testSelectUsingQuestionMarkFlowableParameterListsTwoParametersPerQuery() {
        try (Database db = db()) {
            db.select("select score from person where name=? and score = ?") //
                    .parameterListStream(
                            Flowable.just(Arrays.asList("FRED", 21), Arrays.asList("JOSEPH", 34))) //
                    .getAs(Integer.class) //
                    .test().awaitDone(TIMEOUT_SECONDS, TimeUnit.SECONDS) //
                    .awaitDone(TIMEOUT_SECONDS, TimeUnit.SECONDS) //
                    .assertNoErrors() //
                    .assertValues(21, 34) //
                    .assertComplete();
        }
    }

    @Test
    public void testSelectUsingQuestionMarkWithPublicTestingDatabase() {
        try (Database db = Database.test()) {
            db //
                    .select("select score from person where name=?") //
                    .parameters("FRED", "JOSEPH") //
                    .getAs(Integer.class) //
                    .test() //
                    .awaitDone(TIMEOUT_SECONDS, TimeUnit.SECONDS) //
                    .assertNoErrors() //
                    .assertValues(21, 34) //
                    .assertComplete();
        }
    }

    @Test
    public void testSelectWithFetchSize() {
        try (Database db = db()) {
            db.select("select score from person order by name") //
                    .fetchSize(2) //
                    .getAs(Integer.class) //
                    .test() //
                    .awaitDone(TIMEOUT_SECONDS, TimeUnit.SECONDS) //
                    .assertNoErrors() //
                    .assertValues(21, 34, 25) //
                    .assertComplete();
        }
    }

    @Test
    public void testSelectWithFetchSizeZero() {
        try (Database db = db()) {
            db.select("select score from person order by name") //
                    .fetchSize(0) //
                    .getAs(Integer.class) //
                    .test() //
                    .awaitDone(TIMEOUT_SECONDS, TimeUnit.SECONDS) //
                    .assertNoErrors() //
                    .assertValues(21, 34, 25) //
                    .assertComplete();
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void testSelectWithFetchSizeNegative() {
        try (Database db = db()) {
            db.select("select score from person order by name") //
                    .fetchSize(-1);
        }
    }

    @Test
    public void testSelectUsingNonBlockingBuilder() {
        NonBlockingConnectionPool pool = Pools //
                .nonBlocking() //
                .connectionProvider(DatabaseCreator.connectionProvider()) //
                .maxIdleTime(1, TimeUnit.MINUTES) //
                .idleTimeBeforeHealthCheck(1, TimeUnit.MINUTES) //
                .checkoutRetryInterval(1, TimeUnit.SECONDS) //
                .healthy(c -> c.prepareStatement("select 1").execute()) //
                .returnToPoolDelayAfterHealthCheckFailure(1, TimeUnit.SECONDS) //
                .maxPoolSize(3) //
                .build();

        try (Database db = Database.from(pool)) {
            db.select("select score from person where name=?") //
                    .parameters("FRED", "JOSEPH") //
                    .getAs(Integer.class) //
                    .test() //
                    .awaitDone(TIMEOUT_SECONDS, TimeUnit.SECONDS) //
                    .assertNoErrors() //
                    .assertValues(21, 34) //
                    .assertComplete();
        }
    }

    @Test(timeout = 40000)
    public void testSelectUsingNonBlockingBuilderConcurrencyTest()
            throws InterruptedException, TimeoutException {
        info();
        try {
            try (Database db = db(3)) {
                Scheduler scheduler = Schedulers.from(Executors.newFixedThreadPool(50));
                int n = 10000;
                CountDownLatch latch = new CountDownLatch(n);
                AtomicInteger count = new AtomicInteger();
                for (int i = 0; i < n; i++) {
                    db.select("select score from person where name=?") //
                            .parameters("FRED", "JOSEPH") //
                            .getAs(Integer.class) //
                            .subscribeOn(scheduler) //
                            .toList() //
                            .doOnSuccess(x -> {
                                if (!x.equals(Lists.newArrayList(21, 34))) {
                                    throw new RuntimeException("run broken");
                                } else {
                                    // System.out.println(iCopy + " succeeded");
                                }
                            }) //
                            .doOnSuccess(x -> {
                                count.incrementAndGet();
                                latch.countDown();
                            }) //
                            .doOnError(x -> latch.countDown()) //
                            .subscribe();
                }
                if (!latch.await(20, TimeUnit.SECONDS)) {
                    throw new TimeoutException("timeout");
                }
                assertEquals(n, count.get());
            }
        } finally {
            debug();
        }
    }

    @Test(timeout = 5000)
    public void testSelectConcurrencyTest() throws InterruptedException, TimeoutException {
        debug();
        try {
            try (Database db = db(1)) {
                Scheduler scheduler = Schedulers.from(Executors.newFixedThreadPool(2));
                int n = 2;
                CountDownLatch latch = new CountDownLatch(n);
                AtomicInteger count = new AtomicInteger();
                for (int i = 0; i < n; i++) {
                    db.select("select score from person where name=?") //
                            .parameters("FRED", "JOSEPH") //
                            .getAs(Integer.class) //
                            .subscribeOn(scheduler) //
                            .toList() //
                            .doOnSuccess(x -> {
                                if (!x.equals(Lists.newArrayList(21, 34))) {
                                    throw new RuntimeException("run broken");
                                }
                            }) //
                            .doOnSuccess(x -> {
                                count.incrementAndGet();
                                latch.countDown();
                            }) //
                            .doOnError(x -> latch.countDown()) //
                            .subscribe();
                    log.info("submitted " + i);
                }
                if (!latch.await(5000, TimeUnit.SECONDS)) {
                    throw new TimeoutException("timeout");
                }
                assertEquals(n, count.get());
            }
        } finally {
            debug();
        }
    }

    @Test
    public void testDatabaseClose() {
        try (Database db = db()) {
            db.select("select score from person where name=?") //
                    .parameters("FRED", "JOSEPH") //
                    .getAs(Integer.class) //
                    .test() //
                    .awaitDone(TIMEOUT_SECONDS, TimeUnit.SECONDS) //
                    .assertNoErrors() //
                    .assertValues(21, 34) //
                    .assertComplete();
        }
    }

    @Test
    public void testSelectUsingName() {
        try (Database db = db()) {
            db //
                    .select("select score from person where name=:name") //
                    .parameter("name", "FRED") //
                    .parameter("name", "JOSEPH") //
                    .getAs(Integer.class) //
                    .test() //
                    .awaitDone(TIMEOUT_SECONDS, TimeUnit.SECONDS) //
                    .assertValues(21, 34) //
                    .assertComplete();
        }
    }

    @Test
    public void testSelectUsingNameNotGiven() {
        try (Database db = db()) {
            db //
                    .select("select score from person where name=:name and name<>:name2") //
                    .parameter("name", "FRED") //
                    .parameter("name", "JOSEPH") //
                    .getAs(Integer.class) //
                    .test() //
                    .awaitDone(TIMEOUT_SECONDS, TimeUnit.SECONDS) //
                    .assertError(NamedParameterMissingException.class) //
                    .assertNoValues();
        }
    }

    @Test
    public void testSelectUsingParameterNameNullNameWhenSqlHasNoNames() {
        db() //
                .select("select score from person where name=?") //
                .parameter(Parameter.create("name", "FRED")) //
                .getAs(Integer.class) //
                .test() //
                .awaitDone(TIMEOUT_SECONDS, TimeUnit.SECONDS) //
                .assertError(NamedParameterFoundButSqlDoesNotHaveNamesException.class) //
                .assertNoValues();
    }

    @Test
    public void testUpdateWithNull() {
        try (Database db = db()) {
            db //
                    .update("update person set date_of_birth = :dob") //
                    .parameter(Parameter.create("dob", null)) //
                    .counts() //
                    .test() //
                    .awaitDone(TIMEOUT_SECONDS, TimeUnit.SECONDS) //
                    .assertValue(3) //
                    .assertComplete();
        }
    }

    @Test
    public void testUpdateClobWithNull() {
        try (Database db = db()) {
            insertNullClob(db);
            db //
                    .update("update person_clob set document = :doc") //
                    .parameter("doc", Database.NULL_CLOB) //
                    .counts() //
                    .test() //
                    .awaitDone(TIMEOUT_SECONDS, TimeUnit.SECONDS) //
                    .assertValue(1) //
                    .assertComplete();
        }
    }

    @Test
    public void testUpdateClobWithClob() throws SQLException {
        try (Database db = db()) {
            Clob clob = new JDBCClobFile(new File("src/test/resources/big.txt"));
            insertNullClob(db);
            db //
                    .update("update person_clob set document = :doc") //
                    .parameter("doc", clob) //
                    .counts() //
                    .test() //
                    .awaitDone(TIMEOUT_SECONDS, TimeUnit.SECONDS) //
                    .assertValue(1) //
                    .assertComplete();
        }
    }

    @Test
    public void testUpdateClobWithReader() throws FileNotFoundException {
        try (Database db = db()) {
            Reader reader = new FileReader(new File("src/test/resources/big.txt"));
            insertNullClob(db);
            db //
                    .update("update person_clob set document = :doc") //
                    .parameter("doc", reader) //
                    .counts() //
                    .test() //
                    .awaitDone(TIMEOUT_SECONDS, TimeUnit.SECONDS) //
                    .assertValue(1) //
                    .assertComplete();
        }
    }

    @Test
    public void testUpdateBlobWithBlob() throws SQLException {
        try (Database db = db()) {
            Blob blob = new JDBCBlobFile(new File("src/test/resources/big.txt"));
            insertPersonBlob(db);
            db //
                    .update("update person_blob set document = :doc") //
                    .parameter("doc", blob) //
                    .counts() //
                    .test() //
                    .awaitDone(TIMEOUT_SECONDS, TimeUnit.SECONDS) //
                    .assertValue(1) //
                    .assertComplete();
        }
    }

    @Test
    public void testUpdateBlobWithNull() {
        try (Database db = db()) {
            insertPersonBlob(db);
            db //
                    .update("update person_blob set document = :doc") //
                    .parameter("doc", Database.NULL_BLOB) //
                    .counts() //
                    .test() //
                    .awaitDone(TIMEOUT_SECONDS, TimeUnit.SECONDS) //
                    .assertValue(1) //
                    .assertComplete();
        }
    }

    @Test(expected = NullPointerException.class)
    public void testSelectUsingNullNameInParameter() {
        try (Database db = db()) {
            db //
                    .select("select score from person where name=:name") //
                    .parameter(null, "FRED"); //
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void testSelectUsingNameDoesNotExist() {
        try (Database db = db()) {
            db //
                    .select("select score from person where name=:name") //
                    .parameters("nam", "FRED");
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void testSelectUsingNameWithoutSpecifyingNameThrowsImmediately() {
        try (Database db = db()) {
            db //
                    .select("select score from person where name=:name") //
                    .parameters("FRED", "JOSEPH");
        }
    }

    @Test
    public void testSelectTransacted() {
        try (Database db = db()) {
            db //
                    .select("select score from person where name=?") //
                    .parameters("FRED", "JOSEPH") //
                    .transacted() //
                    .getAs(Integer.class) //
                    .doOnNext(tx -> System.out.println(tx.isComplete() ? "complete" : tx.value())) //
                    .test() //
                    .awaitDone(TIMEOUT_SECONDS, TimeUnit.SECONDS) //
                    .assertValueCount(3) //
                    .assertComplete();
        }
    }

    @Test
    public void testSelectAutomappedAnnotatedTransacted() {
        try (Database db = db()) {
            db //
                    .select(Person10.class) //
                    .transacted() //
                    .valuesOnly() //
                    .get().test() //
                    .awaitDone(TIMEOUT_SECONDS, TimeUnit.SECONDS) //
                    .assertValueCount(3) //
                    .assertComplete();
        }
    }

    @Test
    public void testSelectAutomappedTransactedValuesOnly() {
        try (Database db = db()) {
            db //
                    .select("select name, score from person") //
                    .transacted() //
                    .valuesOnly() //
                    .autoMap(Person2.class) //
                    .test() //
                    .awaitDone(TIMEOUT_SECONDS, TimeUnit.SECONDS) //
                    .assertValueCount(3) //
                    .assertComplete();
        }
    }

    @Test
    public void testSelectAutomappedTransacted() {
        try (Database db = db()) {
            db //
                    .select("select name, score from person") //
                    .transacted() //
                    .autoMap(Person2.class) //
                    .test() //
                    .awaitDone(TIMEOUT_SECONDS, TimeUnit.SECONDS) //
                    .assertValueCount(4) //
                    .assertComplete();
        }
    }

    @Test
    public void testSelectTransactedTuple2() {
        try (Database db = db()) {
            Tx<Tuple2<String, Integer>> t = db //
                    .select("select name, score from person where name=?") //
                    .parameters("FRED") //
                    .transacted() //
                    .getAs(String.class, Integer.class) //
                    .test() //
                    .awaitDone(TIMEOUT_SECONDS, TimeUnit.SECONDS) //
                    .assertValueCount(2) //
                    .assertComplete() //
                    .values().get(0);
            assertEquals("FRED", t.value()._1());
            assertEquals(21, (int) t.value()._2());
        }
    }

    @Test
    public void testSelectTransactedTuple3() {
        try (Database db = db()) {
            Tx<Tuple3<String, Integer, String>> t = db //
                    .select("select name, score, name from person where name=?") //
                    .parameters("FRED") //
                    .transacted() //
                    .getAs(String.class, Integer.class, String.class) //
                    .test() //
                    .awaitDone(TIMEOUT_SECONDS, TimeUnit.SECONDS) //
                    .assertValueCount(2) //
                    .assertComplete() //
                    .values().get(0);
            assertEquals("FRED", t.value()._1());
            assertEquals(21, (int) t.value()._2());
            assertEquals("FRED", t.value()._3());
        }
    }

    @Test
    public void testSelectTransactedTuple4() {
        try (Database db = db()) {
            Tx<Tuple4<String, Integer, String, Integer>> t = db //
                    .select("select name, score, name, score from person where name=?") //
                    .parameters("FRED") //
                    .transacted() //
                    .getAs(String.class, Integer.class, String.class, Integer.class) //
                    .test() //
                    .awaitDone(TIMEOUT_SECONDS, TimeUnit.SECONDS) //
                    .assertValueCount(2) //
                    .assertComplete() //
                    .values().get(0);
            assertEquals("FRED", t.value()._1());
            assertEquals(21, (int) t.value()._2());
            assertEquals("FRED", t.value()._3());
            assertEquals(21, (int) t.value()._4());
        }
    }

    @Test
    public void testSelectTransactedTuple5() {
        try (Database db = db()) {
            Tx<Tuple5<String, Integer, String, Integer, String>> t = db //
                    .select("select name, score, name, score, name from person where name=?") //
                    .parameters("FRED") //
                    .transacted() //
                    .getAs(String.class, Integer.class, String.class, Integer.class, String.class) //
                    .test() //
                    .awaitDone(TIMEOUT_SECONDS, TimeUnit.SECONDS) //
                    .assertValueCount(2) //
                    .assertComplete() //
                    .values().get(0);
            assertEquals("FRED", t.value()._1());
            assertEquals(21, (int) t.value()._2());
            assertEquals("FRED", t.value()._3());
            assertEquals(21, (int) t.value()._4());
            assertEquals("FRED", t.value()._5());
        }
    }

    @Test
    public void testSelectTransactedTuple6() {
        try (Database db = db()) {
            Tx<Tuple6<String, Integer, String, Integer, String, Integer>> t = db //
                    .select("select name, score, name, score, name, score from person where name=?") //
                    .parameters("FRED") //
                    .transacted() //
                    .getAs(String.class, Integer.class, String.class, Integer.class, String.class,
                            Integer.class) //
                    .test() //
                    .awaitDone(TIMEOUT_SECONDS, TimeUnit.SECONDS) //
                    .assertValueCount(2) //
                    .assertComplete() //
                    .values().get(0);
            assertEquals("FRED", t.value()._1());
            assertEquals(21, (int) t.value()._2());
            assertEquals("FRED", t.value()._3());
            assertEquals(21, (int) t.value()._4());
            assertEquals("FRED", t.value()._5());
            assertEquals(21, (int) t.value()._6());
        }
    }

    @Test
    public void testSelectTransactedTuple7() {
        try (Database db = db()) {
            Tx<Tuple7<String, Integer, String, Integer, String, Integer, String>> t = db //
                    .select("select name, score, name, score, name, score, name from person where name=?") //
                    .parameters("FRED") //
                    .transacted() //
                    .getAs(String.class, Integer.class, String.class, Integer.class, String.class,
                            Integer.class, String.class) //
                    .test() //
                    .awaitDone(TIMEOUT_SECONDS, TimeUnit.SECONDS) //
                    .assertValueCount(2) //
                    .assertComplete() //
                    .values().get(0);
            assertEquals("FRED", t.value()._1());
            assertEquals(21, (int) t.value()._2());
            assertEquals("FRED", t.value()._3());
            assertEquals(21, (int) t.value()._4());
            assertEquals("FRED", t.value()._5());
            assertEquals(21, (int) t.value()._6());
            assertEquals("FRED", t.value()._7());
        }
    }

    @Test
    public void testSelectTransactedTupleN() {
        List<Tx<TupleN<Object>>> list = db() //
                .select("select name, score from person where name=?") //
                .parameters("FRED") //
                .transacted() //
                .getTupleN() //
                .test() //
                .awaitDone(TIMEOUT_SECONDS, TimeUnit.SECONDS) //
                .assertValueCount(2) //
                .assertComplete() //
                .values();
        assertEquals("FRED", list.get(0).value().values().get(0));
        assertEquals(21, (int) list.get(0).value().values().get(1));
        assertTrue(list.get(1).isComplete());
        assertEquals(2, list.size());
    }

    @Test
    public void testSelectTransactedCount() {
        db() //
                .select("select name, score, name, score, name, score, name from person where name=?") //
                .parameters("FRED") //
                .transacted() //
                .count() //
                .test() //
                .awaitDone(TIMEOUT_SECONDS, TimeUnit.SECONDS) //
                .assertValueCount(1) //
                .assertComplete();
    }

    @Test
    public void testSelectTransactedGetAs() {
        db() //
                .select("select name from person") //
                .transacted() //
                .getAs(String.class) //
                .test() //
                .awaitDone(TIMEOUT_SECONDS, TimeUnit.SECONDS) //
                .assertValueCount(4) //
                .assertComplete();
    }

    @Test
    public void testSelectTransactedGetAsOptional() {
        List<Tx<Optional<String>>> list = db() //
                .select("select name from person where name=?") //
                .parameters("FRED") //
                .transacted() //
                .getAsOptional(String.class) //
                .test() //
                .awaitDone(TIMEOUT_SECONDS, TimeUnit.SECONDS) //
                .assertValueCount(2) //
                .assertComplete() //
                .values();
        assertTrue(list.get(0).isValue());
        assertEquals("FRED", list.get(0).value().get());
        assertTrue(list.get(1).isComplete());
    }

    @Test
    public void testDatabaseFrom() {
        Database.from(DatabaseCreator.nextUrl(), 3) //
                .select("select name from person") //
                .getAs(String.class) //
                .count() //
                .test() //
                .awaitDone(TIMEOUT_SECONDS, TimeUnit.SECONDS) //
                .assertError(JdbcSQLException.class);
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
                        .parameter(tx.value()) //
                        .valuesOnly() //
                        .getAs(String.class)) //
                .test() //
                .awaitDone(TIMEOUT_SECONDS, TimeUnit.SECONDS) //
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
                            .parameter(score) //
                            .getAs(String.class) //
                            .doOnComplete(() -> log.info("completed select where score=" + score));
                }) //
                .test() //
                .awaitDone(TIMEOUT_SECONDS, TimeUnit.SECONDS) //
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
                .awaitDone(TIMEOUT_SECONDS, TimeUnit.SECONDS) //
                .assertNoValues() //
                .assertError(SQLSyntaxErrorException.class);
    }

    @Test
    public void testReadMeFragmentDerbyHealthCheck() {
        Database db = Database.test();
        db.select("select 'a' from sysibm.sysdummy1") //
                .getAs(String.class) //
                .test() //
                .awaitDone(TIMEOUT_SECONDS, TimeUnit.SECONDS) //
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
                .parameter("FRED") //
                .getAs(Integer.class) //
                .doOnNext(x -> Thread.sleep(1000)) //
                .subscribeOn(Schedulers.io()) //
                .subscribe();
        Thread.sleep(100);
        CountDownLatch latch = new CountDownLatch(1);
        db.select("select score from person where name=?") //
                .parameter("FRED") //
                .getAs(Integer.class) //
                .doOnNext(x -> list.add("emitted")) //
                .doOnNext(x -> System.out.println("emitted on " + Thread.currentThread().getName())) //
                .doOnNext(x -> latch.countDown()) //
                .subscribe();
        list.add("subscribed");
        assertTrue(latch.await(TIMEOUT_SECONDS, TimeUnit.SECONDS));
        assertEquals(Arrays.asList("subscribed", "emitted"), list);
    }

    @Test
    public void testAutoMapToInterface() {
        db() //
                .select("select name from person") //
                .autoMap(Person.class) //
                .map(p -> p.name()) //
                .test() //
                .awaitDone(TIMEOUT_SECONDS, TimeUnit.SECONDS) //
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
                .awaitDone(TIMEOUT_SECONDS, TimeUnit.SECONDS) //
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
                .awaitDone(TIMEOUT_SECONDS, TimeUnit.SECONDS) //
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
                .awaitDone(TIMEOUT_SECONDS, TimeUnit.SECONDS) //
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
                .awaitDone(TIMEOUT_SECONDS, TimeUnit.SECONDS) //
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
                .awaitDone(TIMEOUT_SECONDS, TimeUnit.SECONDS) //
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
                .awaitDone(TIMEOUT_SECONDS, TimeUnit.SECONDS) //
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
                .test().awaitDone(TIMEOUT_SECONDS, TimeUnit.SECONDS) //
                .assertNoValues() //
                .assertError(ColumnIndexOutOfRangeException.class);
    }

    @Test
    public void testAutoMapWithUnmappableColumnType() {
        db() //
                .select("select name from person order by name") //
                .autoMap(Person8.class) //
                .map(p -> p.name()) //
                .test().awaitDone(TIMEOUT_SECONDS, TimeUnit.SECONDS) //
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
                .test().awaitDone(TIMEOUT_SECONDS, TimeUnit.SECONDS) //
                .assertValue(21) //
                .assertComplete();
    }

    @Test
    public void testAutoMapWithQueryInAnnotation() {
        db().select(Person10.class) //
                .get() //
                .firstOrError() //
                .map(Person10::score) //
                .test().awaitDone(TIMEOUT_SECONDS, TimeUnit.SECONDS) //
                .assertValue(21) //
                .assertComplete();
    }

    @Test
    @Ignore
    public void testAutoMapForReadMe() {
        Database //
                .test() //
                .select(Person10.class) //
                .get(Person10::name) //
                .blockingForEach(System.out::println);
    }

    @Test(expected = QueryAnnotationMissingException.class)
    public void testAutoMapWithoutQueryInAnnotation() {
        db().select(Person.class);
    }

    @Test
    public void testSelectWithoutWhereClause() {
        Assert.assertEquals(3, (long) db().select("select name from person") //
                .count() //
                .blockingGet());
    }

    @Test
    public void testTuple3() {
        db() //
                .select("select name, score, name from person order by name") //
                .getAs(String.class, Integer.class, String.class) //
                .firstOrError() //
                .test().awaitDone(TIMEOUT_SECONDS, TimeUnit.SECONDS) //
                .assertComplete() //
                .assertValue(Tuple3.create("FRED", 21, "FRED")); //
    }

    @Test
    public void testTuple4() {
        db() //
                .select("select name, score, name, score from person order by name") //
                .getAs(String.class, Integer.class, String.class, Integer.class) //
                .firstOrError() //
                .test().awaitDone(TIMEOUT_SECONDS, TimeUnit.SECONDS) //
                .assertComplete() //
                .assertValue(Tuple4.create("FRED", 21, "FRED", 21)); //
    }

    @Test
    public void testTuple5() {
        db() //
                .select("select name, score, name, score, name from person order by name") //
                .getAs(String.class, Integer.class, String.class, Integer.class, String.class) //
                .firstOrError() //
                .test().awaitDone(TIMEOUT_SECONDS, TimeUnit.SECONDS) //
                .assertComplete().assertValue(Tuple5.create("FRED", 21, "FRED", 21, "FRED")); //
    }

    @Test
    public void testTuple6() {
        db() //
                .select("select name, score, name, score, name, score from person order by name") //
                .getAs(String.class, Integer.class, String.class, Integer.class, String.class,
                        Integer.class) //
                .firstOrError() //
                .test().awaitDone(TIMEOUT_SECONDS, TimeUnit.SECONDS) //
                .assertComplete().assertValue(Tuple6.create("FRED", 21, "FRED", 21, "FRED", 21)); //
    }

    @Test
    public void testTuple7() {
        db() //
                .select("select name, score, name, score, name, score, name from person order by name") //
                .getAs(String.class, Integer.class, String.class, Integer.class, String.class,
                        Integer.class, String.class) //
                .firstOrError() //
                .test().awaitDone(TIMEOUT_SECONDS, TimeUnit.SECONDS) //
                .assertComplete()
                .assertValue(Tuple7.create("FRED", 21, "FRED", 21, "FRED", 21, "FRED")); //
    }

    @Test
    public void testTupleN() {
        db() //
                .select("select name, score, name from person order by name") //
                .getTupleN() //
                .firstOrError().test().awaitDone(TIMEOUT_SECONDS, TimeUnit.SECONDS) //
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
                .test().awaitDone(TIMEOUT_SECONDS, TimeUnit.SECONDS) //
                .assertValue(1) //
                .assertComplete();
    }

    @Test
    public void testUpdateThreeRows() {
        db().update("update person set score=20") //
                .counts() //
                .test().awaitDone(TIMEOUT_SECONDS, TimeUnit.SECONDS) //
                .assertValue(3) //
                .assertComplete();
    }

    @Test
    public void testUpdateWithParameter() {
        db().update("update person set score=20 where name=?") //
                .parameter("FRED").counts() //
                .test().awaitDone(TIMEOUT_SECONDS, TimeUnit.SECONDS) //
                .assertValue(1) //
                .assertComplete();
    }

    @Test
    public void testUpdateWithParameterTwoRuns() {
        db().update("update person set score=20 where name=?") //
                .parameters("FRED", "JOSEPH").counts() //
                .test().awaitDone(TIMEOUT_SECONDS, TimeUnit.SECONDS) //
                .assertValues(1, 1) //
                .assertComplete();
    }

    @Test
    public void testUpdateAllWithParameterFourRuns() {
        db().update("update person set score=?") //
                .parameters(1, 2, 3, 4) //
                .counts() //
                .test().awaitDone(TIMEOUT_SECONDS, TimeUnit.SECONDS) //
                .assertValues(3, 3, 3, 3) //
                .assertComplete();
    }

    @Test
    public void testUpdateWithBatchSize2() {
        db().update("update person set score=?") //
                .batchSize(2) //
                .parameters(1, 2, 3, 4) //
                .counts() //
                .test().awaitDone(TIMEOUT_SECONDS, TimeUnit.SECONDS) //
                .assertValues(3, 3, 3, 3) //
                .assertComplete();
    }

    @Test
    public void testUpdateWithBatchSize3GreaterThanNumRecords() {
        db().update("update person set score=?") //
                .batchSize(3) //
                .parameters(1, 2, 3, 4) //
                .counts() //
                .test().awaitDone(TIMEOUT_SECONDS, TimeUnit.SECONDS) //
                .assertValues(3, 3, 3, 3) //
                .assertComplete();
    }

    @Test
    public void testInsert() {
        Database db = db();
        db.update("insert into person(name, score) values(?,?)") //
                .parameters("DAVE", 12, "ANNE", 18) //
                .counts() //
                .test().awaitDone(TIMEOUT_SECONDS, TimeUnit.SECONDS) //
                .assertValues(1, 1) //
                .assertComplete();
        List<Tuple2<String, Integer>> list = db.select("select name, score from person") //
                .getAs(String.class, Integer.class) //
                .toList() //
                .timeout(TIMEOUT_SECONDS, TimeUnit.SECONDS) //
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
                .awaitDone(TIMEOUT_SECONDS, TimeUnit.SECONDS) //
                .assertValues(1, 2) //
                .assertComplete();

        db.update("insert into note(text) values(?)") //
                .parameters("ME", "TOO") //
                .returnGeneratedKeys() //
                .getAs(Integer.class)//
                .test().awaitDone(TIMEOUT_SECONDS, TimeUnit.SECONDS) //
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
                .awaitDone(TIMEOUT_SECONDS, TimeUnit.SECONDS) //
                .assertNoErrors().assertValues(1, 3) //
                .assertComplete();

        db.update("insert into note2(text) values(?)") //
                .parameters("ME", "TOO") //
                .returnGeneratedKeys() //
                .getAs(Integer.class)//
                .test() //
                .awaitDone(TIMEOUT_SECONDS, TimeUnit.SECONDS) //
                .assertValues(5, 7) //
                .assertComplete();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testReturnGeneratedKeysWithBatchSizeShouldThrow() {
        Database db = db();
        // note is a table with auto increment
        db.update("insert into note(text) values(?)") //
                .parameters("HI", "THERE") //
                .batchSize(2) //
                .returnGeneratedKeys();
    }

    @Test
    public void testTransactedReturnGeneratedKeys() {
        Database db = db();
        // note is a table with auto increment
        db.update("insert into note(text) values(?)") //
                .parameters("HI", "THERE") //
                .transacted() //
                .returnGeneratedKeys() //
                .valuesOnly() //
                .getAs(Integer.class)//
                .test() //
                .awaitDone(TIMEOUT_SECONDS, TimeUnit.SECONDS) //
                .assertValues(1, 2) //
                .assertComplete();

        db.update("insert into note(text) values(?)") //
                .parameters("ME", "TOO") //
                .transacted() //
                .returnGeneratedKeys() //
                .valuesOnly() //
                .getAs(Integer.class)//
                .test() //
                .awaitDone(TIMEOUT_SECONDS, TimeUnit.SECONDS) //
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
                .valuesOnly() //
                .getAs(Integer.class);

        db.update("insert into note(text) values(?)") //
                .parameters("ME", "TOO") //
                .transacted() //
                .returnGeneratedKeys() //
                .valuesOnly() //
                .getAs(Integer.class)//
                .startWith(a) //
                .test() //
                .awaitDone(TIMEOUT_SECONDS, TimeUnit.SECONDS) //
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
                .awaitDone(TIMEOUT_SECONDS, TimeUnit.SECONDS) //
                .assertValues(1, 1, 1) //
                .assertComplete();
    }

    @Test
    public void testSelectDependsOnFlowable() {
        Database db = db();
        Flowable<Integer> a = db.update("update person set score=100 where name=?") //
                .parameter("FRED") //
                .counts();
        db.select("select score from person where name=?") //
                .parameter("FRED") //
                .dependsOn(a) //
                .getAs(Integer.class)//
                .test().awaitDone(TIMEOUT_SECONDS, TimeUnit.SECONDS) //
                .assertValues(100) //
                .assertComplete();
    }

    @Test
    public void testSelectDependsOnObservable() {
        Database db = db();
        Observable<Integer> a = db.update("update person set score=100 where name=?") //
                .parameter("FRED") //
                .counts().toObservable();
        db.select("select score from person where name=?") //
                .parameter("FRED") //
                .dependsOn(a) //
                .getAs(Integer.class)//
                .test().awaitDone(TIMEOUT_SECONDS, TimeUnit.SECONDS) //
                .assertValues(100) //
                .assertComplete();
    }

    @Test
    public void testSelectDependsOnOnSingle() {
        Database db = db();
        Single<Long> a = db.update("update person set score=100 where name=?") //
                .parameter("FRED") //
                .counts().count();
        db.select("select score from person where name=?") //
                .parameter("FRED") //
                .dependsOn(a) //
                .getAs(Integer.class)//
                .test().awaitDone(TIMEOUT_SECONDS, TimeUnit.SECONDS) //
                .assertValues(100) //
                .assertComplete();
    }

    @Test
    public void testSelectDependsOnCompletable() {
        Database db = db();
        Completable a = db.update("update person set score=100 where name=?") //
                .parameter("FRED") //
                .counts().ignoreElements();
        db.select("select score from person where name=?") //
                .parameter("FRED") //
                .dependsOn(a) //
                .getAs(Integer.class)//
                .test().awaitDone(TIMEOUT_SECONDS, TimeUnit.SECONDS) //
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
                .test().awaitDone(TIMEOUT_SECONDS, TimeUnit.SECONDS) //
                .assertValues(1, 1, 1) //
                .assertComplete();
    }

    private static void info() {
        LogManager.getRootLogger().setLevel(Level.INFO);
    }

    private static void debug() {
        LogManager.getRootLogger().setLevel(Level.INFO);
    }

    @Test
    public void testCreateBig() {
        info();
        big(5).select("select count(*) from person") //
                .getAs(Integer.class) //
                .test().awaitDone(20, TimeUnit.SECONDS) //
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
                .test().awaitDone(20, TimeUnit.SECONDS) //
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
                .test().awaitDone(20, TimeUnit.SECONDS) //
                .assertValue((long) NAMES_COUNT_BIG) //
                .assertComplete();
        debug();
    }

    @Test
    public void testInsertNullClobAndReadClobAsString() {
        Database db = db();
        insertNullClob(db);
        db.select("select document from person_clob where name='FRED'") //
                .getAsOptional(String.class) //
                .test().awaitDone(TIMEOUT_SECONDS, TimeUnit.SECONDS) //
                .assertValue(Optional.<String>empty()) //
                .assertComplete();
    }

    private static void insertNullClob(Database db) {
        db.update("insert into person_clob(name,document) values(?,?)") //
                .parameters("FRED", Database.NULL_CLOB) //
                .counts() //
                .test().awaitDone(TIMEOUT_SECONDS, TimeUnit.SECONDS) //
                .assertValue(1) //
                .assertComplete();
    }

    @Test
    public void testClobMethod() {
        assertEquals(Database.NULL_CLOB, Database.clob(null));
    }

    @Test
    public void testBlobMethod() {
        assertEquals(Database.NULL_BLOB, Database.blob(null));
    }

    @Test
    public void testClobMethodPresent() {
        assertEquals("a", Database.clob("a"));
    }

    @Test
    public void testBlobMethodPresent() {
        byte[] b = new byte[1];
        assertEquals(b, Database.blob(b));
    }

    @Test
    public void testDateOfBirthNullableForReadMe() {
        Database.test() //
                .select("select date_of_birth from person where name='FRED'") //
                .getAsOptional(Instant.class) //
                .blockingForEach(System.out::println);
    }

    @Test
    public void testInsertNullClobAndReadClobAsTuple2() {
        Database db = db();
        insertNullClob(db);
        db.select("select document, document from person_clob where name='FRED'") //
                .getAs(String.class, String.class) //
                .test().awaitDone(TIMEOUT_SECONDS, TimeUnit.SECONDS) //
                .assertValue(Tuple2.create(null, null)) //
                .assertComplete();
    }

    @Test
    public void testInsertClobAndReadClobAsString() {
        Database db = db();
        db.update("insert into person_clob(name,document) values(?,?)") //
                .parameters("FRED", "some text here") //
                .counts() //
                .test().awaitDone(TIMEOUT_SECONDS, TimeUnit.SECONDS) //
                .assertValue(1) //
                .assertComplete();
        db.select("select document from person_clob where name='FRED'") //
                .getAs(String.class) //
                .test().awaitDone(TIMEOUT_SECONDS, TimeUnit.SECONDS) // .assertValue("some
                                                                     // text
                                                                     // here")
                                                                     // //
                .assertComplete();
    }

    @Test
    public void testInsertClobAndReadClobUsingReader() {
        Database db = db();
        db.update("insert into person_clob(name,document) values(?,?)") //
                .parameters("FRED", "some text here") //
                .counts() //
                .test().awaitDone(TIMEOUT_SECONDS, TimeUnit.SECONDS) //
                .assertValue(1) //
                .assertComplete();
        db.select("select document from person_clob where name='FRED'") //
                .getAs(Reader.class) //
                .map(r -> read(r)).test().awaitDone(TIMEOUT_SECONDS, TimeUnit.SECONDS) //
                .assertValue("some text here") //
                .assertComplete();
    }

    @Test
    public void testInsertBlobAndReadBlobAsByteArray() {
        Database db = db();
        insertPersonBlob(db);
        db.select("select document from person_blob where name='FRED'") //
                .getAs(byte[].class) //
                .map(b -> new String(b)) //
                .test().awaitDone(TIMEOUT_SECONDS, TimeUnit.SECONDS) //
                .assertValue("some text here") //
                .assertComplete();
    }

    private static void insertPersonBlob(Database db) {
        byte[] bytes = "some text here".getBytes();
        db.update("insert into person_blob(name,document) values(?,?)") //
                .parameters("FRED", bytes) //
                .counts() //
                .test().awaitDone(TIMEOUT_SECONDS, TimeUnit.SECONDS) //
                .assertValue(1) //
                .assertComplete();
    }

    @Test
    public void testInsertBlobAndReadBlobAsInputStream() {
        Database db = db();
        byte[] bytes = "some text here".getBytes();
        db.update("insert into person_blob(name,document) values(?,?)") //
                .parameters("FRED", new ByteArrayInputStream(bytes)) //
                .counts() //
                .test().awaitDone(TIMEOUT_SECONDS, TimeUnit.SECONDS) //
                .assertValue(1) //
                .assertComplete();
        db.select("select document from person_blob where name='FRED'") //
                .getAs(InputStream.class) //
                .map(is -> read(is)) //
                .map(b -> new String(b)) //
                .test().awaitDone(TIMEOUT_SECONDS, TimeUnit.SECONDS) //
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
            TestSubscriber<Integer> ts0 = db.select( //
                    "select score from person where name=?") //
                    .parameter("FRED") //
                    .getAs(Integer.class) //
                    .test();
            ts0.assertValueCount(0) //
                    .assertNotComplete();
            scheduler.advanceTimeBy(1, TimeUnit.MINUTES);
            ts0.assertValueCount(1) //
                    .assertComplete();
            TestSubscriber<Integer> ts = db.select( //
                    "select score from person where name=?") //
                    .parameter("FRED") //
                    .getAs(Integer.class) //
                    .test() //
                    .assertValueCount(0);
            System.out.println("done2");
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
                .parameter("FRED") //
                .getAs(Integer.class) //
                .test().awaitDone(TIMEOUT_SECONDS, TimeUnit.SECONDS) //
                .assertNoValues() //
                .assertError(PoolClosedException.class);
    }

    @Test
    public void testFewerColumnsMappedThanAvailable() {
        db().select("select name, score from person where name='FRED'") //
                .getAs(String.class) //
                .test().awaitDone(TIMEOUT_SECONDS, TimeUnit.SECONDS) //
                .assertValues("FRED") //
                .assertComplete();
    }

    @Test
    public void testMoreColumnsMappedThanAvailable() {
        db() //
                .select("select name, score from person where name='FRED'") //
                .getAs(String.class, Integer.class, String.class) //
                .test().awaitDone(TIMEOUT_SECONDS, TimeUnit.SECONDS) //
                .assertNoValues() //
                .assertError(MoreColumnsRequestedThanExistException.class);
    }

    @Test
    public void testSelectTimestamp() {
        db() //
                .select("select registered from person where name='FRED'") //
                .getAs(Long.class) //
                .test().awaitDone(TIMEOUT_SECONDS, TimeUnit.SECONDS) //
                .assertValue(FRED_REGISTERED_TIME) //
                .assertComplete();
    }

    @Test
    public void testSelectTimestampAsDate() {
        db() //
                .select("select registered from person where name='FRED'") //
                .getAs(Date.class) //
                .map(d -> d.getTime()) //
                .test().awaitDone(TIMEOUT_SECONDS, TimeUnit.SECONDS) //
                .assertValue(FRED_REGISTERED_TIME) //
                .assertComplete();
    }

    @Test
    public void testSelectTimestampAsInstant() {
        db() //
                .select("select registered from person where name='FRED'") //
                .getAs(Instant.class) //
                .map(d -> d.toEpochMilli()) //
                .test().awaitDone(TIMEOUT_SECONDS, TimeUnit.SECONDS) //
                .assertValue(FRED_REGISTERED_TIME) //
                .assertComplete();
    }

    @Test
    public void testUpdateCalendarParameter() {
        Calendar c = GregorianCalendar.from(ZonedDateTime.parse("2017-03-25T15:37Z"));
        Database db = db();
        db.update("update person set registered=?") //
                .parameter(c) //
                .counts() //
                .test() //
                .awaitDone(TIMEOUT_SECONDS, TimeUnit.SECONDS) //
                .assertValue(3) //
                .assertComplete();
        db.select("select registered from person") //
                .getAs(Long.class) //
                .firstOrError() //
                .test() //
                .awaitDone(TIMEOUT_SECONDS, TimeUnit.SECONDS) //
                .assertValue(c.getTimeInMillis()) //
                .assertComplete();
    }

    @Test
    public void testUpdateTimeParameter() {
        Database db = db();
        Time t = new Time(1234);
        db.update("update person set registered=?") //
                .parameter(t) //
                .counts() //
                .test() //
                .awaitDone(TIMEOUT_SECONDS, TimeUnit.SECONDS) //
                .assertValue(3) //
                .assertComplete();
        db.select("select registered from person") //
                .getAs(Long.class) //
                .firstOrError() //
                .test() //
                .awaitDone(TIMEOUT_SECONDS, TimeUnit.SECONDS) //
                .assertValue(1234L) //
                .assertComplete();
    }

    @Test
    public void testUpdateTimestampParameter() {
        Database db = db();
        Timestamp t = new Timestamp(1234);
        db.update("update person set registered=?") //
                .parameter(t) //
                .counts() //
                .test() //
                .awaitDone(TIMEOUT_SECONDS, TimeUnit.SECONDS) //
                .assertValue(3) //
                .assertComplete();
        db.select("select registered from person") //
                .getAs(Long.class) //
                .firstOrError() //
                .test() //
                .awaitDone(TIMEOUT_SECONDS, TimeUnit.SECONDS) //
                .assertValue(1234L) //
                .assertComplete();
    }

    @Test
    public void testUpdateSqlDateParameter() {
        Database db = db();
        java.sql.Date t = new java.sql.Date(1234);

        db.update("update person set registered=?") //
                .parameter(t) //
                .counts() //
                .test() //
                .awaitDone(TIMEOUT_SECONDS, TimeUnit.SECONDS) //
                .assertValue(3) //
                .assertComplete();
        db.select("select registered from person") //
                .getAs(Long.class) //
                .firstOrError() //
                .test() //
                .awaitDone(TIMEOUT_SECONDS * 10000, TimeUnit.SECONDS) //
                // TODO make a more accurate comparison using the current TZ
                .assertValue(x -> Math.abs(x - 1234) <= TimeUnit.HOURS.toMillis(24)) //
                .assertComplete();
    }

    @Test
    public void testUpdateUtilDateParameter() {
        Database db = db();
        Date d = new Date(1234);
        db.update("update person set registered=?") //
                .parameter(d) //
                .counts() //
                .test() //
                .awaitDone(TIMEOUT_SECONDS, TimeUnit.SECONDS) //
                .assertValue(3) //
                .assertComplete();
        db.select("select registered from person") //
                .getAs(Long.class) //
                .firstOrError() //
                .test() //
                .awaitDone(TIMEOUT_SECONDS, TimeUnit.SECONDS) //
                .assertValue(1234L) //
                .assertComplete();
    }

    @Test
    public void testUpdateTimestampAsInstant() {
        Database db = db();
        db.update("update person set registered=? where name='FRED'") //
                .parameter(Instant.ofEpochMilli(FRED_REGISTERED_TIME)) //
                .counts() //
                .test().awaitDone(TIMEOUT_SECONDS, TimeUnit.SECONDS) //
                .assertValue(1) //
                .assertComplete();
        db.select("select registered from person where name='FRED'") //
                .getAs(Long.class) //
                .test().awaitDone(TIMEOUT_SECONDS, TimeUnit.SECONDS) //
                .assertValue(FRED_REGISTERED_TIME) //
                .assertComplete();
    }

    @Test
    public void testUpdateTimestampAsZonedDateTime() {
        Database db = db();
        db.update("update person set registered=? where name='FRED'") //
                .parameter(ZonedDateTime.ofInstant(Instant.ofEpochMilli(FRED_REGISTERED_TIME),
                        ZoneOffset.UTC.normalized())) //
                .counts() //
                .test().awaitDone(TIMEOUT_SECONDS, TimeUnit.SECONDS) //
                .assertValue(1) //
                .assertComplete();
        db.select("select registered from person where name='FRED'") //
                .getAs(Long.class) //
                .test().awaitDone(TIMEOUT_SECONDS, TimeUnit.SECONDS) //
                .assertValue(FRED_REGISTERED_TIME) //
                .assertComplete();
    }

    @Test
    public void testCompleteCompletes() {
        Database db = db(1);
        db //
                .update("update person set score=-3 where name='FRED'") //
                .complete() //
                .timeout(TIMEOUT_SECONDS, TimeUnit.SECONDS) //
                .blockingAwait();

        int score = db.select("select score from person where name='FRED'") //
                .getAs(Integer.class) //
                .timeout(TIMEOUT_SECONDS, TimeUnit.SECONDS) //
                .blockingFirst();
        assertEquals(-3, score);
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
                .test().awaitDone(TIMEOUT_SECONDS, TimeUnit.SECONDS) //
                .assertValue(1) //
                .assertComplete();
    }

    @Test
    public void testCountsOnlyInTransaction() {
        db().update("update person set score = -3") //
                .transacted() //
                .countsOnly() //
                .test().awaitDone(TIMEOUT_SECONDS, TimeUnit.SECONDS) //
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
                .test().awaitDone(TIMEOUT_SECONDS, TimeUnit.SECONDS) //
                .assertValue(list -> list.get(0).isValue() && list.get(0).value() == 3
                        && list.get(1).isComplete() && list.size() == 2) //
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
                .test().awaitDone(TIMEOUT_SECONDS, TimeUnit.SECONDS) //
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
                            .parameter(tx.value()) //
                            .countsOnly() //
                            .doOnSubscribe(s -> System.out.println("subscribed")) //
                            .doOnNext(num -> System.out.println("num=" + num));
                }) //
                .test().awaitDone(TIMEOUT_SECONDS, TimeUnit.SECONDS) //
                .assertNoErrors() //
                .assertValue(1) //
                .assertComplete();
    }

    @Test
    public void testSingleFlatMap() {
        Single.just(1).flatMapPublisher(n -> Flowable.just(1)).test(1).assertValue(1)
                .assertComplete();
    }

    @Test
    public void testAutomappedInstanceHasMeaningfulToStringMethod() {
        info();
        String s = Database.test() //
                .select("select name, score from person where name=?") //
                .parameterStream(Flowable.just("FRED")) //
                .autoMap(Person2.class) //
                .map(x -> x.toString()) //
                .blockingSingle();
        assertEquals("Person2[name=FRED, score=21]", s);
    }

    @Test
    public void testAutomappedEquals() {
        boolean b = Database.test() //
                .select("select name, score from person where name=?") //
                .parameterStream(Flowable.just("FRED")) //
                .autoMap(Person2.class) //
                .map(x -> x.equals(x)) //
                .blockingSingle();
        assertTrue(b);
    }

    @Test
    public void testAutomappedDoesNotEqualNull() {
        boolean b = Database.test() //
                .select("select name, score from person where name=?") //
                .parameterStream(Flowable.just("FRED")) //
                .autoMap(Person2.class) //
                .map(x -> x.equals(null)) //
                .blockingSingle();
        assertFalse(b);
    }

    @Test
    public void testAutomappedDoesNotEqual() {
        boolean b = Database.test() //
                .select("select name, score from person where name=?") //
                .parameterStream(Flowable.just("FRED")) //
                .autoMap(Person2.class) //
                .map(x -> x.equals(new Object())) //
                .blockingSingle();
        assertFalse(b);
    }

    @Test
    public void testAutomappedHashCode() {
        Person2 p = Database.test() //
                .select("select name, score from person where name=?") //
                .parameterStream(Flowable.just("FRED")) //
                .autoMap(Person2.class) //
                .blockingSingle();
        assertTrue(p.hashCode() != 0);
    }

    @Test
    public void testAutomappedWithParametersThatProvokesMoreThanOneQuery() {
        Database.test() //
                .select("select name, score from person where name=?") //
                .parameters("FRED", "FRED") //
                .autoMap(Person2.class) //
                .doOnNext(System.out::println) //
                .test() //
                .awaitDone(2000, TimeUnit.SECONDS) //
                .assertNoErrors() //
                .assertValueCount(2) //
                .assertComplete();
    }

    @Test
    public void testAutomappedObjectsEqualsAndHashCodeIsDistinctOnValues() {
        Database.test() //
                .select("select name, score from person where name=?") //
                .parameters("FRED", "FRED") //
                .autoMap(Person2.class) //
                .distinct() //
                .doOnNext(System.out::println) //
                .test() //
                .awaitDone(2000, TimeUnit.SECONDS) //
                .assertNoErrors() //
                .assertValueCount(1) //
                .assertComplete();
    }

    @Test
    public void testAutomappedObjectsEqualsDifferentiatesDifferentInterfacesWithSameMethodNamesAndValues() {
        PersonDistinct1 p1 = Database.test() //
                .select("select name, score from person where name=?") //
                .parameters("FRED") //
                .autoMap(PersonDistinct1.class) //
                .blockingFirst();

        PersonDistinct2 p2 = Database.test() //
                .select("select name, score from person where name=?") //
                .parameters("FRED") //
                .autoMap(PersonDistinct2.class) //
                .blockingFirst();

        assertNotEquals(p1, p2);
    }

    @Test
    public void testAutomappedObjectsWhenDefaultMethodInvoked() {
        PersonWithDefaultMethod p = Database.test() //
                .select("select name, score from person where name=?") //
                .parameters("FRED") //
                .autoMap(PersonWithDefaultMethod.class) //
                .blockingFirst();
        assertEquals("fred", p.nameLower());
    }

    // @Test
    // public void testDefaultMethod() throws NoSuchMethodException,
    // IllegalAccessException {
    // MethodHandles.lookup().findSpecial(PersonWithDefaultMethod.class,
    // "nameLower",
    // MethodType.methodType(String.class, new Class[] {}),
    // PersonWithDefaultMethod.class)
    //// .bindTo(x);
    // }

    @Test(expected = AutomappedInterfaceInaccessibleException.class)
    public void testAutomappedObjectsWhenDefaultMethodInvokedAndIsNonPublicThrows() {
        PersonWithDefaultMethodNonPublic p = Database.test() //
                .select("select name, score from person where name=?") //
                .parameters("FRED") //
                .autoMap(PersonWithDefaultMethodNonPublic.class) //
                .blockingFirst();
        p.nameLower();
    }

    @Test
    public void testBlockingDatabase() {
        Database db = blocking();
        db.select("select score from person where name=?") //
                .parameters("FRED", "JOSEPH") //
                .getAs(Integer.class) //
                .test() //
                .awaitDone(TIMEOUT_SECONDS, TimeUnit.SECONDS) //
                .assertNoErrors() //
                .assertValues(21, 34) //
                .assertComplete();
    }

    @Test
    public void testBlockingDatabaseTransacted() {
        Database db = blocking();
        db.select("select score from person where name=?") //
                .parameters("FRED", "JOSEPH") //
                .transactedValuesOnly() //
                .getAs(Integer.class) //
                .map(x -> x.value()) //
                .test() //
                .awaitDone(TIMEOUT_SECONDS, TimeUnit.SECONDS) //
                .assertNoErrors() //
                .assertValues(21, 34) //
                .assertComplete();
    }

    @Test
    public void testBlockingDatabaseTransactedNested() {
        Database db = blocking();
        db.select("select score from person where name=?") //
                .parameters("FRED", "JOSEPH") //
                .transactedValuesOnly() //
                .getAs(Integer.class) //
                .flatMap(tx -> tx.select("select name from person where score=?") //
                        .parameter(tx.value()) //
                        .valuesOnly() //
                        .getAs(String.class))
                .test() //
                .awaitDone(TIMEOUT_SECONDS, TimeUnit.SECONDS) //
                .assertNoErrors() //
                .assertValues("FRED", "JOSEPH") //
                .assertComplete();
    }

    public interface PersonWithDefaultMethod {
        @Column
        String name();

        @Column
        int score();

        public default String nameLower() {
            return name().toLowerCase();
        }
    }

    interface PersonWithDefaultMethodNonPublic {
        @Column
        String name();

        @Column
        int score();

        default String nameLower() {
            return name().toLowerCase();
        }
    }

    interface PersonDistinct1 {
        @Column
        String name();

        @Column
        int score();

    }

    interface PersonDistinct2 {
        @Column
        String name();

        @Column
        int score();

    }

    interface Score {
        @Column
        int score();
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
