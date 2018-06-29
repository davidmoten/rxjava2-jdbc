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
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLSyntaxErrorException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
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
import org.davidmoten.rx.jdbc.exceptions.CannotForkTransactedConnection;
import org.davidmoten.rx.jdbc.exceptions.ColumnIndexOutOfRangeException;
import org.davidmoten.rx.jdbc.exceptions.ColumnNotFoundException;
import org.davidmoten.rx.jdbc.exceptions.MoreColumnsRequestedThanExistException;
import org.davidmoten.rx.jdbc.exceptions.NamedParameterFoundButSqlDoesNotHaveNamesException;
import org.davidmoten.rx.jdbc.exceptions.NamedParameterMissingException;
import org.davidmoten.rx.jdbc.exceptions.QueryAnnotationMissingException;
import org.davidmoten.rx.jdbc.exceptions.SQLRuntimeException;
import org.davidmoten.rx.jdbc.internal.DelegatedConnection;
import org.davidmoten.rx.jdbc.pool.DatabaseCreator;
import org.davidmoten.rx.jdbc.pool.DatabaseType;
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
import com.github.davidmoten.guavamini.Sets;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.reactivex.Completable;
import io.reactivex.Flowable;
import io.reactivex.Observable;
import io.reactivex.Scheduler;
import io.reactivex.Single;
import io.reactivex.exceptions.UndeliverableException;
import io.reactivex.functions.Predicate;
import io.reactivex.plugins.RxJavaPlugins;
import io.reactivex.schedulers.Schedulers;
import io.reactivex.schedulers.TestScheduler;
import io.reactivex.subscribers.TestSubscriber;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class DatabaseTest {

    private static final long FRED_REGISTERED_TIME = 1442515672690L;
    private static final int NAMES_COUNT_BIG = 5163;
    private static final Logger log = LoggerFactory.getLogger(DatabaseTest.class);
    private static final int TIMEOUT_SECONDS = 10;

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
    public void testDemonstrateDbClosureOnTerminate() {
        Database db = db();
        db.select("select score from person where name=?") //
                .parameters("FRED", "JOSEPH") //
                .getAs(Integer.class) //
                .doOnTerminate(() -> db.close()) //
                .test() //
                .awaitDone(TIMEOUT_SECONDS, TimeUnit.SECONDS) //
                .assertNoErrors() //
                .assertValues(21, 34) //
                .assertComplete();
    }

    @Test
    public void testFromDataSource() {
        ConnectionProvider cp = DatabaseCreator.connectionProvider();
        AtomicInteger count = new AtomicInteger();
        Set<Connection> connections = new HashSet<>();
        AtomicInteger closed = new AtomicInteger();
        Database db = Database.fromBlocking(new ConnectionProvider() {

            @Override
            public Connection get() {
                count.incrementAndGet();
                Connection c = cp.get();
                connections.add(c);
                return new DelegatedConnection() {

                    @Override
                    public Connection con() {
                        return c;
                    }

                    @Override
                    public void close() {
                        closed.incrementAndGet();
                    }
                };
            }

            @Override
            public void close() {
                // do nothing
            }
        });
        db.select("select count(*) from person") //
                .getAs(Integer.class) //
                .blockingSubscribe();
        db.select("select count(*) from person") //
                .getAs(Integer.class) //
                .blockingSubscribe();
        assertEquals(2, count.get());
        assertEquals(2, connections.size());
        assertEquals(2, closed.get());
    }

    @Test
    public void testBlockingForEachWhenError() {
        try {
            Flowable //
                    .error(new RuntimeException("boo")) //
                    .blockingForEach(System.out::println);
        } catch (RuntimeException e) {
            assertEquals("boo", e.getMessage());
        }
    }

    @Test
    public void testSelectUsingQuestionMarkAndInClauseIssue10() {
        Database.test() //
                .select("select score from person where name in (?) order by score") //
                .parameters("FRED", "JOSEPH") //
                .getAs(Integer.class) //
                .test() //
                .awaitDone(TIMEOUT_SECONDS, TimeUnit.SECONDS) //
                .assertNoErrors() //
                .assertValues(21, 34) //
                .assertComplete();
    }

    @Test
    public void testSelectUsingQuestionMarkAndInClauseWithSetParameter() {
        Database.test() //
                .select("select score from person where name in (?) order by score") //
                .parameter(Sets.newHashSet("FRED", "JOSEPH")) //
                .getAs(Integer.class) //
                .test() //
                .awaitDone(TIMEOUT_SECONDS, TimeUnit.SECONDS) //
                .assertNoErrors() //
                .assertValues(21, 34) //
                .assertComplete();
    }

    @Test
    public void testUpdateWithInClauseBatchSize0() {
        Database.test() //
                .update("update person set score=50 where name in (?)") //
                .batchSize(0) //
                .parameter(Sets.newHashSet("FRED", "JOSEPH")) //
                .counts() //
                .test() //
                .awaitDone(TIMEOUT_SECONDS, TimeUnit.DAYS) //
                .assertComplete() //
                .assertValues(2);
    }

    @Test
    public void testUpdateWithInClauseBatchSize10() {
        Database.test() //
                .update("update person set score=50 where name in (?)") //
                .batchSize(10) //
                .parameter(Sets.newHashSet("FRED", "JOSEPH")) //
                .counts() //
                .test() //
                .awaitDone(TIMEOUT_SECONDS, TimeUnit.DAYS) //
                .assertComplete() //
                .assertValues(2);
    }

    @Test
    public void testSelectUsingQuestionMarkAndInClauseWithSetParameterUsingParametersMethod() {
        Database.test() //
                .select("select score from person where name in (?) order by score") //
                .parameters(Sets.newHashSet("FRED", "JOSEPH")) //
                .getAs(Integer.class) //
                .test() //
                .awaitDone(TIMEOUT_SECONDS, TimeUnit.SECONDS) //
                .assertNoErrors() //
                .assertValues(21, 34) //
                .assertComplete();
    }

    @Test
    public void testSelectUsingInClauseWithListParameter() {
        Database.test() //
                .select("select score from person where score > ? and name in (?) order by score") //
                .parameters(0, Lists.newArrayList("FRED", "JOSEPH")) //
                .getAs(Integer.class) //
                .test() //
                .awaitDone(TIMEOUT_SECONDS, TimeUnit.SECONDS) //
                .assertNoErrors() //
                .assertValues(21, 34) //
                .assertComplete();
    }

    @Test
    public void testSelectUsingInClauseWithNamedListParameter() {
        Database.test() //
                .select("select score from person where score > :score and name in (:names) order by score") //
                .parameter("score", 0) //
                .parameter("names", Lists.newArrayList("FRED", "JOSEPH")) //
                .getAs(Integer.class) //
                .test() //
                .awaitDone(TIMEOUT_SECONDS, TimeUnit.SECONDS) //
                .assertNoErrors() //
                .assertValues(21, 34) //
                .assertComplete();
    }

    @Test
    public void testSelectUsingInClauseWithRepeatedNamedListParameter() {
        Database.test() //
                .select("select score from person where name in (:names) and name in (:names) order by score") //
                .parameter("names", Lists.newArrayList("FRED", "JOSEPH")) //
                .getAs(Integer.class) //
                .test() //
                .awaitDone(TIMEOUT_SECONDS, TimeUnit.SECONDS) //
                .assertNoErrors() //
                .assertValues(21, 34) //
                .assertComplete();
    }

    @Test
    public void testSelectUsingInClauseWithRepeatedNamedListParameterAndRepeatedNonListParameter() {
        Database.test() //
                .select("select score from person where name in (:names) and score >:score and name in (:names) and score >:score order by score") //
                .parameter("names", Lists.newArrayList("FRED", "JOSEPH")) //
                .parameter("score", 0) //
                .getAs(Integer.class) //
                .test() //
                .awaitDone(TIMEOUT_SECONDS, TimeUnit.SECONDS) //
                .assertNoErrors() //
                .assertValues(21, 34) //
                .assertComplete();
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
                .connectionRetryInterval(1, TimeUnit.SECONDS) //
                .healthCheck(c -> c.prepareStatement("select 1").execute()) //
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

    @Test
    public void testSelectSpecifyingHealthCheck() {
        try (Database db = Database//
                .nonBlocking() //
                .connectionProvider(DatabaseCreator.connectionProvider()) //
                .maxIdleTime(1, TimeUnit.MINUTES) //
                .idleTimeBeforeHealthCheck(1, TimeUnit.MINUTES) //
                .connectionRetryInterval(1, TimeUnit.SECONDS) //
                .healthCheck(DatabaseType.H2) //
                .maxPoolSize(3) //
                .build()) {
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

    @Test(expected = IllegalArgumentException.class)
    public void testCreateTestDatabaseWithZeroSizePoolThrows() {
        Database.test(0);
    }

    @Test
    public void testSelectSpecifyingHealthCheckAsSql() {
        final AtomicInteger success = new AtomicInteger();
        NonBlockingConnectionPool pool = Pools //
                .nonBlocking() //
                .connectionProvider(DatabaseCreator.connectionProvider()) //
                .maxIdleTime(1, TimeUnit.MINUTES) //
                .healthCheck(DatabaseType.H2) //
                .idleTimeBeforeHealthCheck(1, TimeUnit.MINUTES) //
                .connectionRetryInterval(1, TimeUnit.SECONDS) //
                .healthCheck("select 1") //
                .connectionListener(error -> {
                    if (error.isPresent()) {
                        success.set(Integer.MIN_VALUE);
                    } else {
                        success.incrementAndGet();
                    }
                }) //
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
        assertEquals(1, success.get());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNonBlockingPoolWithTrampolineSchedulerThrows() {
        Pools.nonBlocking().scheduler(Schedulers.trampoline());
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
                                    // log.debug(iCopy + " succeeded");
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
    public void testUpdateWithNullNamedParameter() {
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
    public void testUpdateWithNullParameter() {
        try (Database db = db()) {
            db //
                    .update("update person set date_of_birth = ?") //
                    .parameter(null) //
                    .counts() //
                    .test() //
                    .awaitDone(TIMEOUT_SECONDS, TimeUnit.SECONDS) //
                    .assertValue(3) //
                    .assertComplete();
        }
    }

    @Test
    public void testUpdateWithNullStreamParameter() {
        try (Database db = db()) {
            db //
                    .update("update person set date_of_birth = ?") //
                    .parameterStream(Flowable.just(Parameter.NULL)) //
                    .counts() //
                    .test() //
                    .awaitDone(TIMEOUT_SECONDS, TimeUnit.SECONDS) //
                    .assertValue(3) //
                    .assertComplete();
        }
    }

    @Test
    public void testUpdateWithTestDatabaseForReadme() {
        try (Database db = db()) {
            db //
                    .update("update person set date_of_birth = ?") //
                    .parameterStream(Flowable.just(Parameter.NULL)) //
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
                    .doOnNext(tx -> log
                            .debug(tx.isComplete() ? "complete" : String.valueOf(tx.value()))) //
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
        try (Database db = db()) {
            List<Tx<TupleN<Object>>> list = db //
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
    }

    @Test
    public void testSelectTransactedCount() {
        try (Database db = db()) {
            db //
                    .select("select name, score, name, score, name, score, name from person where name=?") //
                    .parameters("FRED") //
                    .transacted() //
                    .count() //
                    .test() //
                    .awaitDone(TIMEOUT_SECONDS, TimeUnit.SECONDS) //
                    .assertValueCount(1) //
                    .assertComplete();
        }
    }

    @Test
    public void testSelectTransactedGetAs() {
        try (Database db = db()) {
            db //
                    .select("select name from person") //
                    .transacted() //
                    .getAs(String.class) //
                    .test() //
                    .awaitDone(TIMEOUT_SECONDS, TimeUnit.SECONDS) //
                    .assertValueCount(4) //
                    .assertComplete();
        }
    }

    @Test
    public void testSelectTransactedGetAsOptional() {
        try (Database db = db()) {
            List<Tx<Optional<String>>> list = db //
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
        try (Database db = db()) {
            db //
                    .select("select score from person where name=?") //
                    .parameters("FRED", "JOSEPH") //
                    .transacted() //
                    .transactedValuesOnly() //
                    .getAs(Integer.class) //
                    .doOnNext(tx -> log
                            .debug(tx.isComplete() ? "complete" : String.valueOf(tx.value())))//
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
    }

    @Test
    public void databaseIsAutoCloseable() {
        try (Database db = db()) {
            log.debug(db.toString());
        }
    }

    private static void println(Object o) {
        log.debug("{}", o);
    }

    @Test
    public void testSelectChained() {
        try (Database db = db(1)) {
            // we can do this with 1 connection only!
            db.select("select score from person where name=?") //
                    .parameters("FRED", "JOSEPH") //
                    .getAs(Integer.class) //
                    .doOnNext(DatabaseTest::println) //
                    .concatMap(score -> {
                        log.info("score={}", score);
                        return db //
                                .select("select name from person where score = ?") //
                                .parameter(score) //
                                .getAs(String.class) //
                                .doOnComplete(
                                        () -> log.info("completed select where score=" + score));
                    }) //
                    .test() //
                    .awaitDone(TIMEOUT_SECONDS, TimeUnit.SECONDS) //
                    .assertNoErrors() //
                    .assertValues("FRED", "JOSEPH") //
                    .assertComplete(); //
        }
    }

    @Test
    @SuppressFBWarnings
    public void testReadMeFragment1() {
        try (Database db = db()) {
            db.select("select name from person") //
                    .getAs(String.class) //
                    .forEach(DatabaseTest::println);
        }
    }

    @Test
    public void testReadMeFragmentColumnDoesNotExistEmitsSqlSyntaxErrorException() {
        try (Database db = Database.test()) {
            db.select("select nam from person") //
                    .getAs(String.class) //
                    .test() //
                    .awaitDone(TIMEOUT_SECONDS, TimeUnit.SECONDS) //
                    .assertNoValues() //
                    .assertError(SQLSyntaxErrorException.class);
        }
    }

    @Test
    public void testReadMeFragmentDerbyHealthCheck() {
        try (Database db = Database.test()) {
            db.select("select 'a' from sysibm.sysdummy1") //
                    .getAs(String.class) //
                    .test() //
                    .awaitDone(TIMEOUT_SECONDS, TimeUnit.SECONDS) //
                    .assertValue("a") //
                    .assertComplete();
        }
    }

    @Test
    @SuppressFBWarnings
    public void testTupleSupport() {
        try (Database db = db()) {
            db.select("select name, score from person") //
                    .getAs(String.class, Integer.class) //
                    .forEach(DatabaseTest::println);
        }
    }

    @Test
    public void testDelayedCallsAreNonBlocking() throws InterruptedException {
        List<String> list = new CopyOnWriteArrayList<String>();
        try (Database db = db(1)) { //
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
                    .doOnNext(x -> log.debug("emitted on " + Thread.currentThread().getName())) //
                    .doOnNext(x -> latch.countDown()) //
                    .subscribe();
            list.add("subscribed");
            assertTrue(latch.await(TIMEOUT_SECONDS, TimeUnit.SECONDS));
            assertEquals(Arrays.asList("subscribed", "emitted"), list);
        }
    }

    @Test
    public void testAutoMapToInterface() {
        try (Database db = db()) {
            db //
                    .select("select name from person") //
                    .autoMap(Person.class) //
                    .map(p -> p.name()) //
                    .test() //
                    .awaitDone(TIMEOUT_SECONDS, TimeUnit.SECONDS) //
                    .assertValueCount(3) //
                    .assertComplete();
        }
    }

    @Test
    public void testAutoMapToInterfaceWithoutAnnotationstsError() {
        try (Database db = db()) {
            db //
                    .select("select name from person") //
                    .autoMap(PersonNoAnnotation.class) //
                    .map(p -> p.name()) //
                    .test() //
                    .awaitDone(TIMEOUT_SECONDS, TimeUnit.SECONDS) //
                    .assertNoValues() //
                    .assertError(AnnotationsNotFoundException.class);
        }
    }

    @Test
    public void testAutoMapToInterfaceWithTwoMethods() {
        try (Database db = db()) {
            db //
                    .select("select name, score from person order by name") //
                    .autoMap(Person2.class) //
                    .firstOrError() //
                    .map(Person2::score) //
                    .test() //
                    .awaitDone(TIMEOUT_SECONDS, TimeUnit.SECONDS) //
                    .assertValue(21) //
                    .assertComplete();
        }
    }

    @Test
    public void testAutoMapToInterfaceWithExplicitColumnName() {
        try (Database db = db()) {
            db //
                    .select("select name, score from person order by name") //
                    .autoMap(Person3.class) //
                    .firstOrError() //
                    .map(Person3::examScore) //
                    .test() //
                    .awaitDone(TIMEOUT_SECONDS, TimeUnit.SECONDS) //
                    .assertValue(21) //
                    .assertComplete();
        }
    }

    @Test
    public void testAutoMapToInterfaceWithExplicitColumnNameThatDoesNotExist() {
        try (Database db = db()) {
            db //
                    .select("select name, score from person order by name") //
                    .autoMap(Person4.class) //
                    .firstOrError() //
                    .map(Person4::examScore) //
                    .test() //
                    .awaitDone(TIMEOUT_SECONDS, TimeUnit.SECONDS) //
                    .assertNoValues() //
                    .assertError(ColumnNotFoundException.class);
        }
    }

    @Test
    public void testAutoMapToInterfaceWithIndex() {
        try (Database db = db()) {
            db //
                    .select("select name, score from person order by name") //
                    .autoMap(Person5.class) //
                    .firstOrError() //
                    .map(Person5::examScore) //
                    .test() //
                    .awaitDone(TIMEOUT_SECONDS, TimeUnit.SECONDS) //
                    .assertValue(21) //
                    .assertComplete();
        }
    }

    @Test
    public void testAutoMapToInterfaceWithIndexTooLarge() {
        try (Database db = db()) {
            db //
                    .select("select name, score from person order by name") //
                    .autoMap(Person6.class) //
                    .firstOrError() //
                    .map(Person6::examScore) //
                    .test() //
                    .awaitDone(TIMEOUT_SECONDS, TimeUnit.SECONDS) //
                    .assertNoValues() //
                    .assertError(ColumnIndexOutOfRangeException.class);
        }
    }

    @Test
    public void testAutoMapToInterfaceWithIndexTooSmall() {
        try (Database db = db()) {
            db //
                    .select("select name, score from person order by name") //
                    .autoMap(Person7.class) //
                    .firstOrError() //
                    .map(Person7::examScore) //
                    .test().awaitDone(TIMEOUT_SECONDS, TimeUnit.SECONDS) //
                    .assertNoValues() //
                    .assertError(ColumnIndexOutOfRangeException.class);
        }
    }

    @Test
    public void testAutoMapWithUnmappableColumnType() {
        try (Database db = db()) {
            db //
                    .select("select name from person order by name") //
                    .autoMap(Person8.class) //
                    .map(p -> p.name()) //
                    .test().awaitDone(TIMEOUT_SECONDS, TimeUnit.SECONDS) //
                    .assertNoValues() //
                    .assertError(ClassCastException.class);
        }
    }

    @Test
    public void testAutoMapWithMixIndexAndName() {
        try (Database db = db()) {
            db //
                    .select("select name, score from person order by name") //
                    .autoMap(Person9.class) //
                    .firstOrError() //
                    .map(Person9::score) //
                    .test().awaitDone(TIMEOUT_SECONDS, TimeUnit.SECONDS) //
                    .assertValue(21) //
                    .assertComplete();
        }
    }

    @Test
    public void testAutoMapWithQueryInAnnotation() {
        try (Database db = db()) {
            db.select(Person10.class) //
                    .get() //
                    .firstOrError() //
                    .map(Person10::score) //
                    .test().awaitDone(TIMEOUT_SECONDS, TimeUnit.SECONDS) //
                    .assertValue(21) //
                    .assertComplete();
        }
    }

    @Test
    @Ignore
    public void testAutoMapForReadMe() {
        try (Database db = Database.test()) {
            db.select(Person10.class) //
                    .get(Person10::name) //
                    .blockingForEach(DatabaseTest::println);
        }
    }

    @Test(expected = QueryAnnotationMissingException.class)
    public void testAutoMapWithoutQueryInAnnotation() {
        try (Database db = db()) {
            db.select(Person.class);
        }
    }

    @Test
    public void testSelectWithoutWhereClause() {
        try (Database db = db()) {
            Assert.assertEquals(3, (long) db.select("select name from person") //
                    .count() //
                    .blockingGet());
        }
    }

    @Test
    public void testTuple3() {
        try (Database db = db()) {
            db //
                    .select("select name, score, name from person order by name") //
                    .getAs(String.class, Integer.class, String.class) //
                    .firstOrError() //
                    .test().awaitDone(TIMEOUT_SECONDS, TimeUnit.SECONDS) //
                    .assertComplete() //
                    .assertValue(Tuple3.create("FRED", 21, "FRED")); //
        }
    }

    @Test
    public void testTuple4() {
        try (Database db = db()) {
            db //
                    .select("select name, score, name, score from person order by name") //
                    .getAs(String.class, Integer.class, String.class, Integer.class) //
                    .firstOrError() //
                    .test().awaitDone(TIMEOUT_SECONDS, TimeUnit.SECONDS) //
                    .assertComplete() //
                    .assertValue(Tuple4.create("FRED", 21, "FRED", 21)); //
        }
    }

    @Test
    public void testTuple5() {
        try (Database db = db()) {
            db //
                    .select("select name, score, name, score, name from person order by name") //
                    .getAs(String.class, Integer.class, String.class, Integer.class, String.class) //
                    .firstOrError() //
                    .test().awaitDone(TIMEOUT_SECONDS, TimeUnit.SECONDS) //
                    .assertComplete().assertValue(Tuple5.create("FRED", 21, "FRED", 21, "FRED")); //
        }
    }

    @Test
    public void testTuple6() {
        try (Database db = db()) {
            db //
                    .select("select name, score, name, score, name, score from person order by name") //
                    .getAs(String.class, Integer.class, String.class, Integer.class, String.class,
                            Integer.class) //
                    .firstOrError() //
                    .test().awaitDone(TIMEOUT_SECONDS, TimeUnit.SECONDS) //
                    .assertComplete()
                    .assertValue(Tuple6.create("FRED", 21, "FRED", 21, "FRED", 21)); //
        }
    }

    @Test
    public void testTuple7() {
        try (Database db = db()) {
            db //
                    .select("select name, score, name, score, name, score, name from person order by name") //
                    .getAs(String.class, Integer.class, String.class, Integer.class, String.class,
                            Integer.class, String.class) //
                    .firstOrError() //
                    .test().awaitDone(TIMEOUT_SECONDS, TimeUnit.SECONDS) //
                    .assertComplete()
                    .assertValue(Tuple7.create("FRED", 21, "FRED", 21, "FRED", 21, "FRED")); //
        }
    }

    @Test
    public void testTupleN() {
        try (Database db = db()) {
            db //
                    .select("select name, score, name from person order by name") //
                    .getTupleN() //
                    .firstOrError().test().awaitDone(TIMEOUT_SECONDS, TimeUnit.SECONDS) //
                    .assertComplete() //
                    .assertValue(TupleN.create("FRED", 21, "FRED")); //
        }
    }

    @Test
    public void testTupleNWithClass() {
        try (Database db = db()) {
            db //
                    .select("select score a, score b from person order by name") //
                    .getTupleN(Integer.class) //
                    .firstOrError() //
                    .test().awaitDone(TIMEOUT_SECONDS, TimeUnit.SECONDS) //
                    .assertComplete() //
                    .assertValue(TupleN.create(21, 21)); //
        }
    }

    @Test
    public void testTupleNWithClassInTransaction() {
        try (Database db = db()) {
            db //
                    .select("select score a, score b from person order by name") //
                    .transactedValuesOnly() //
                    .getTupleN(Integer.class) //
                    .map(x -> x.value()) //
                    .firstOrError() //
                    .test().awaitDone(TIMEOUT_SECONDS, TimeUnit.SECONDS) //
                    .assertComplete() //
                    .assertValue(TupleN.create(21, 21)); //
        }
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
        try (Database db = db()) {
            db.update("update person set score=20 where name='FRED'") //
                    .counts() //
                    .test().awaitDone(TIMEOUT_SECONDS, TimeUnit.SECONDS) //
                    .assertValue(1) //
                    .assertComplete();
        }
    }

    @Test
    public void testUpdateThreeRows() {
        try (Database db = db()) {
            db.update("update person set score=20") //
                    .counts() //
                    .test().awaitDone(TIMEOUT_SECONDS, TimeUnit.SECONDS) //
                    .assertValue(3) //
                    .assertComplete();
        }
    }

    @Test
    public void testUpdateWithParameter() {
        try (Database db = db()) {
            db.update("update person set score=20 where name=?") //
                    .parameter("FRED").counts() //
                    .test().awaitDone(TIMEOUT_SECONDS, TimeUnit.SECONDS) //
                    .assertValue(1) //
                    .assertComplete();
        }
    }

    @Test
    public void testUpdateWithParameterTwoRuns() {
        try (Database db = db()) {
            db.update("update person set score=20 where name=?") //
                    .parameters("FRED", "JOSEPH").counts() //
                    .test().awaitDone(TIMEOUT_SECONDS, TimeUnit.SECONDS) //
                    .assertValues(1, 1) //
                    .assertComplete();
        }
    }

    @Test
    public void testUpdateAllWithParameterFourRuns() {
        try (Database db = db()) {
            db.update("update person set score=?") //
                    .parameters(1, 2, 3, 4) //
                    .counts() //
                    .test().awaitDone(TIMEOUT_SECONDS, TimeUnit.SECONDS) //
                    .assertValues(3, 3, 3, 3) //
                    .assertComplete();
        }
    }

    @Test
    public void testUpdateWithBatchSize2() {
        try (Database db = db()) {
            db.update("update person set score=?") //
                    .batchSize(2) //
                    .parameters(1, 2, 3, 4) //
                    .counts() //
                    .test().awaitDone(TIMEOUT_SECONDS, TimeUnit.SECONDS) //
                    .assertValues(3, 3, 3, 3) //
                    .assertComplete();
        }
    }

    @Test
    public void testUpdateWithBatchSize3GreaterThanNumRecords() {
        try (Database db = db()) {
            db.update("update person set score=?") //
                    .batchSize(3) //
                    .parameters(1, 2, 3, 4) //
                    .counts() //
                    .test().awaitDone(TIMEOUT_SECONDS, TimeUnit.SECONDS) //
                    .assertValues(3, 3, 3, 3) //
                    .assertComplete();
        }
    }

    @Test
    public void testInsert() {
        try (Database db = db()) {
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
    }

    @Test
    public void testReturnGeneratedKeys() {
        try (Database db = db()) {
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
        try (Database db = db()) {
            // note is a table with auto increment
            db.update("insert into note(text) values(?)") //
                    .parameters("HI", "THERE") //
                    .batchSize(2) //
                    .returnGeneratedKeys();
        }
    }

    @Test
    public void testTransactedReturnGeneratedKeys() {
        try (Database db = db()) {
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
    }

    @Test
    public void testTransactedReturnGeneratedKeys2() {
        try (Database db = db()) {
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
    }

    @Test
    public void testUpdateWithinTransaction() {
        try (Database db = db()) {
            db //
                    .select("select name from person") //
                    .transactedValuesOnly() //
                    .getAs(String.class) //
                    .doOnNext(DatabaseTest::println) //
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
    }

    @Test
    public void testSelectDependsOnFlowable() {
        try (Database db = db()) {
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
    }

    @Test
    public void testSelectDependsOnObservable() {
        try (Database db = db()) {
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
    }

    @Test
    public void testSelectDependsOnOnSingle() {
        try (Database db = db()) {
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
    }

    @Test
    public void testSelectDependsOnCompletable() {
        try (Database db = db()) {
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
    }

    @Test
    public void testUpdateWithinTransactionBatchSize0() {
        try (Database db = db()) {
            db //
                    .select("select name from person") //
                    .transactedValuesOnly() //
                    .getAs(String.class) //
                    .doOnNext(DatabaseTest::println) //
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
        try (Database db = db()) {
            insertNullClob(db);
            db.select("select document from person_clob where name='FRED'") //
                    .getAsOptional(String.class) //
                    .test().awaitDone(TIMEOUT_SECONDS, TimeUnit.SECONDS) //
                    .assertValue(Optional.<String>empty()) //
                    .assertComplete();
        }
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
                .blockingForEach(DatabaseTest::println);
    }

    @Test
    public void testInsertNullClobAndReadClobAsTuple2() {
        try (Database db = db()) {
            insertNullClob(db);
            db.select("select document, document from person_clob where name='FRED'") //
                    .getAs(String.class, String.class) //
                    .test().awaitDone(TIMEOUT_SECONDS, TimeUnit.SECONDS) //
                    .assertValue(Tuple2.create(null, null)) //
                    .assertComplete();
        }
    }

    @Test
    public void testInsertClobAndReadClobAsString() {
        try (Database db = db()) {
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
    }

    @Test
    public void testInsertClobAndReadClobUsingReader() {
        try (Database db = db()) {
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
    }

    @Test
    public void testInsertBlobAndReadBlobAsByteArray() {
        try (Database db = db()) {
            insertPersonBlob(db);
            db.select("select document from person_blob where name='FRED'") //
                    .getAs(byte[].class) //
                    .map(b -> new String(b)) //
                    .test().awaitDone(TIMEOUT_SECONDS, TimeUnit.SECONDS) //
                    .assertValue("some text here") //
                    .assertComplete();
        }
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
        try (Database db = db()) {
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
                .healthCheck(healthy) //
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
            log.debug("done2");
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
        try (Database db = db()) {
            db.select("select name, score from person where name='FRED'") //
                    .getAs(String.class) //
                    .test().awaitDone(TIMEOUT_SECONDS, TimeUnit.SECONDS) //
                    .assertValues("FRED") //
                    .assertComplete();
        }
    }

    @Test
    public void testMoreColumnsMappedThanAvailable() {
        try (Database db = db()) {
            db //
                    .select("select name, score from person where name='FRED'") //
                    .getAs(String.class, Integer.class, String.class) //
                    .test().awaitDone(TIMEOUT_SECONDS, TimeUnit.SECONDS) //
                    .assertNoValues() //
                    .assertError(MoreColumnsRequestedThanExistException.class);
        }
    }

    @Test
    public void testSelectTimestamp() {
        try (Database db = db()) {
            db //
                    .select("select registered from person where name='FRED'") //
                    .getAs(Long.class) //
                    .test().awaitDone(TIMEOUT_SECONDS, TimeUnit.SECONDS) //
                    .assertValue(FRED_REGISTERED_TIME) //
                    .assertComplete();
        }
    }

    @Test
    public void testSelectTimestampAsDate() {
        try (Database db = db()) {
            db //
                    .select("select registered from person where name='FRED'") //
                    .getAs(Date.class) //
                    .map(d -> d.getTime()) //
                    .test().awaitDone(TIMEOUT_SECONDS, TimeUnit.SECONDS) //
                    .assertValue(FRED_REGISTERED_TIME) //
                    .assertComplete();
        }
    }

    @Test
    public void testSelectTimestampAsInstant() {
        try (Database db = db()) {
            db //
                    .select("select registered from person where name='FRED'") //
                    .getAs(Instant.class) //
                    .map(d -> d.toEpochMilli()) //
                    .test().awaitDone(TIMEOUT_SECONDS, TimeUnit.SECONDS) //
                    .assertValue(FRED_REGISTERED_TIME) //
                    .assertComplete();
        }
    }

    @Test
    public void testUpdateCalendarParameter() {
        Calendar c = GregorianCalendar.from(ZonedDateTime.parse("2017-03-25T15:37Z"));
        try (Database db = db()) {
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
    }

    @Test
    public void testUpdateTimeParameter() {
        try (Database db = db()) {
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
    }

    @Test
    public void testUpdateTimestampParameter() {
        try (Database db = db()) {
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
    }

    @Test
    public void testUpdateSqlDateParameter() {
        try (Database db = db()) {
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
                    .awaitDone(TIMEOUT_SECONDS, TimeUnit.SECONDS) //
                    // TODO make a more accurate comparison using the current TZ
                    .assertValue(x -> Math.abs(x - 1234) <= TimeUnit.HOURS.toMillis(24)) //
                    .assertComplete();
        }
    }

    @Test
    public void testUpdateUtilDateParameter() {
        try (Database db = db()) {
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
    }

    @Test
    public void testUpdateTimestampAsInstant() {
        try (Database db = db()) {
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
    }

    @Test
    public void testUpdateTimestampAsZonedDateTime() {
        try (Database db = db()) {
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
    }

    @Test
    public void testCompleteCompletes() {
        try (Database db = db(1)) {
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
    }

    @Test
    public void testComplete() throws InterruptedException {
        try (Database db = db(1)) {
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
    }

    @Test
    public void testCountsOnlyInTransaction() {
        try (Database db = db()) {
            db.update("update person set score = -3") //
                    .transacted() //
                    .countsOnly() //
                    .test().awaitDone(TIMEOUT_SECONDS, TimeUnit.SECONDS) //
                    .assertValues(3) //
                    .assertComplete();
        }
    }

    @Test
    public void testCountsInTransaction() {
        try (Database db = db()) {
            db.update("update person set score = -3") //
                    .transacted() //
                    .counts() //
                    .doOnNext(DatabaseTest::println) //
                    .toList() //
                    .test().awaitDone(TIMEOUT_SECONDS, TimeUnit.SECONDS) //
                    .assertValue(list -> list.get(0).isValue() && list.get(0).value() == 3
                            && list.get(1).isComplete() && list.size() == 2) //
                    .assertComplete();
        }
    }

    @Test
    public void testTx() throws InterruptedException {
        Database db = db(3);
        Flowable<Tx<?>> transaction = db //
                .update("update person set score=-3 where name='FRED'") //
                .transaction();

        transaction //
                .doOnCancel(() -> log.debug("disposing")) //
                .doOnNext(DatabaseTest::println) //
                .flatMap(tx -> {
                    log.debug("flatmapping");
                    return tx //
                            .update("update person set score = -4 where score = -3") //
                            .countsOnly() //
                            .doOnSubscribe(s -> log.debug("subscribed")) //
                            .doOnNext(num -> log.debug("num=" + num));
                }) //
                .test() //
                .awaitDone(TIMEOUT_SECONDS, TimeUnit.SECONDS) //
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
                .doOnDispose(() -> log.debug("disposing")) //
                .doOnSuccess(DatabaseTest::println) //
                .flatMapPublisher(tx -> {
                    log.debug("flatmapping");
                    return tx //
                            .update("update person set score = -4 where score = ?") //
                            .parameter(tx.value()) //
                            .countsOnly() //
                            .doOnSubscribe(s -> log.debug("subscribed")) //
                            .doOnNext(num -> log.debug("num=" + num));
                }) //
                .test().awaitDone(TIMEOUT_SECONDS, TimeUnit.SECONDS) //
                .assertNoErrors() //
                .assertValue(1) //
                .assertComplete();
    }

    @Test
    public void testUseTxOnComplete() {
        db(1) //
                .select(Person10.class) //
                .transacted() //
                .get() //
                .lastOrError() //
                .map(tx -> tx.select("select count(*) from person") //
                        .count().blockingGet()) //
                .test() //
                .awaitDone(TIMEOUT_SECONDS, TimeUnit.SECONDS) //
                .assertError(CannotForkTransactedConnection.class) //
                .assertValueCount(0);
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
                .doOnNext(DatabaseTest::println) //
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
                .doOnNext(DatabaseTest::println) //
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
        // only run test if java 8
        if (System.getProperty("java.version").startsWith("1.8.")) {
            PersonWithDefaultMethod p = Database.test() //
                    .select("select name, score from person where name=?") //
                    .parameters("FRED") //
                    .autoMap(PersonWithDefaultMethod.class) //
                    .blockingFirst();
            assertEquals("fred", p.nameLower());
        }
    }

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

    @Test
    public void testUsingNormalJDBCApi() {
        Database db = db(1);
        db.apply(con -> {
            try (PreparedStatement stmt = con
                    .prepareStatement("select count(*) from person where name='FRED'");
                    ResultSet rs = stmt.executeQuery()) {
                rs.next();
                return rs.getInt(1);
            }
        }) //
                .test() //
                .awaitDone(TIMEOUT_SECONDS, TimeUnit.SECONDS) //
                .assertValue(1) //
                .assertComplete();

        // now check that the connection was returned to the pool
        db.select("select count(*) from person where name='FRED'") //
                .getAs(Integer.class) //
                .test() //
                .awaitDone(TIMEOUT_SECONDS, TimeUnit.SECONDS) //
                .assertValue(1) //
                .assertComplete();
    }

    @Test
    public void testUsingNormalJDBCApiCompletable() {
        Database db = db(1);
        db.apply(con -> {
            try (PreparedStatement stmt = con
                    .prepareStatement("select count(*) from person where name='FRED'");
                    ResultSet rs = stmt.executeQuery()) {
                rs.next();
            }
        }) //
                .test() //
                .awaitDone(TIMEOUT_SECONDS, TimeUnit.SECONDS) //
                .assertComplete();

        // now check that the connection was returned to the pool
        db.select("select count(*) from person where name='FRED'") //
                .getAs(Integer.class) //
                .test() //
                .awaitDone(TIMEOUT_SECONDS, TimeUnit.SECONDS) //
                .assertValue(1) //
                .assertComplete();
    }

    @Test
    public void testCallableStatement() {
        Database db = DatabaseCreator.createDerbyWithStoredProcs(1);
        db.apply(con -> {
            try (Statement stmt = con.createStatement()) {
                CallableStatement st = con.prepareCall("call getPersonCount(?, ?)");
                st.setInt(1, 0);
                st.registerOutParameter(2, Types.INTEGER);
                st.execute();
                assertEquals(2, st.getInt(2));
            }
        }).blockingAwait(TIMEOUT_SECONDS, TimeUnit.SECONDS);
    }

    @Test
    public void testCallableStatementReturningResultSets() {
        Database db = DatabaseCreator.createDerbyWithStoredProcs(1);
        db.apply(con -> {
            try (Statement stmt = con.createStatement()) {
                CallableStatement st = con.prepareCall("call in1out0rs2(?)");
                st.setInt(1, 0);
                st.execute();
                ResultSet rs1 = st.getResultSet();
                st.getMoreResults(Statement.KEEP_CURRENT_RESULT);
                ResultSet rs2 = st.getResultSet();
                rs1.next();
                assertEquals("FRED", rs1.getString(1));
                rs2.next();
                assertEquals("SARAH", rs2.getString(1));
            }
        }).blockingAwait(TIMEOUT_SECONDS, TimeUnit.SECONDS);
    }

    @Test
    public void testCallableApiNoParameters() {
        Database db = DatabaseCreator.createDerbyWithStoredProcs(1);
        db //
                .call("call zero()") //
                .once() //
                .test() //
                .awaitDone(TIMEOUT_SECONDS, TimeUnit.SECONDS) //
                .assertComplete();
    }

    @Test
    public void testCallableApiNoParametersTransacted() {
        Database db = DatabaseCreator.createDerbyWithStoredProcs(1);
        db //
                .call("call zero()") //
                .transacted() //
                .once() //
                .test() //
                .awaitDone(TIMEOUT_SECONDS, TimeUnit.SECONDS) //
                .assertValueCount(1) //
                .assertValue(x -> x.isComplete()) //
                .assertComplete();
    }

    @Test
    public void testCallableApiOneInOutParameter() {
        Database db = DatabaseCreator.createDerbyWithStoredProcs(1);
        db //
                .call("call inout1(?)") //
                .inOut(Type.INTEGER, Integer.class) //
                .input(4) //
                .test() //
                .awaitDone(TIMEOUT_SECONDS, TimeUnit.SECONDS) //
                .assertValue(5) //
                .assertComplete();
    }

    @Test
    public void testCallableApiOneInOutParameterTransacted() {
        Database db = DatabaseCreator.createDerbyWithStoredProcs(1);
        db //
                .call("call inout1(?)") //
                .transacted() //
                .inOut(Type.INTEGER, Integer.class) //
                .input(4) //
                .flatMap(Tx.flattenToValuesOnly()) //
                .test() //
                .awaitDone(TIMEOUT_SECONDS, TimeUnit.SECONDS) //
                .assertValue(5) //
                .assertComplete();
    }

    @Test
    public void testCallableApiTwoInOutParameters() {
        Database db = DatabaseCreator.createDerbyWithStoredProcs(1);
        db //
                .call("call inout2(?, ?)") //
                .inOut(Type.INTEGER, Integer.class) //
                .inOut(Type.INTEGER, Integer.class) //
                .input(4, 10) //
                .test() //
                .awaitDone(TIMEOUT_SECONDS, TimeUnit.SECONDS) //
                .assertValue(x -> x._1() == 5 && x._2() == 12) //
                .assertComplete();
    }

    @Test
    public void testCallableApiTwoInOutParametersTransacted() {
        Database db = DatabaseCreator.createDerbyWithStoredProcs(1);
        db //
                .call("call inout2(?, ?)") //
                .transacted() //
                .inOut(Type.INTEGER, Integer.class) //
                .inOut(Type.INTEGER, Integer.class) //
                .input(4, 10) //
                .flatMap(Tx.flattenToValuesOnly()) //
                .test() //
                .awaitDone(TIMEOUT_SECONDS, TimeUnit.SECONDS) //
                .assertValue(x -> x._1() == 5 && x._2() == 12) //
                .assertComplete();
    }

    @Test
    public void testCallableApiThreeInOutParameters() {
        Database db = DatabaseCreator.createDerbyWithStoredProcs(1);
        db //
                .call("call inout3(?, ?, ?)") //
                .inOut(Type.INTEGER, Integer.class) //
                .inOut(Type.INTEGER, Integer.class) //
                .inOut(Type.INTEGER, Integer.class) //
                .input(4, 10, 13) //
                .test() //
                .awaitDone(TIMEOUT_SECONDS, TimeUnit.SECONDS) //
                .assertValue(x -> x._1() == 5 && x._2() == 12 && x._3() == 16) //
                .assertComplete();
    }

    @Test
    public void testCallableApiThreeInOutParametersTransacted() {
        Database db = DatabaseCreator.createDerbyWithStoredProcs(1);
        db //
                .call("call inout3(?, ?, ?)") //
                .transacted() //
                .inOut(Type.INTEGER, Integer.class) //
                .inOut(Type.INTEGER, Integer.class) //
                .inOut(Type.INTEGER, Integer.class) //
                .input(4, 10, 13) //
                .flatMap(Tx.flattenToValuesOnly()) //
                .test() //
                .awaitDone(TIMEOUT_SECONDS, TimeUnit.SECONDS) //
                .assertValue(x -> x._1() == 5 && x._2() == 12 && x._3() == 16) //
                .assertComplete();
    }

    @Test
    public void testCallableApiReturningOneOutParameter() throws InterruptedException {
        Database db = DatabaseCreator.createDerbyWithStoredProcs(1);
        db //
                .call("call in1out1(?,?)") //
                .in() //
                .out(Type.INTEGER, Integer.class) //
                .input(0, 10, 20) //
                .test() //
                .awaitDone(TIMEOUT_SECONDS, TimeUnit.SECONDS) //
                .assertValues(0, 10, 20) //
                .assertComplete();
    }

    @Test
    public void testCallableApiReturningOneOutParameterTransacted() throws InterruptedException {
        Database db = DatabaseCreator.createDerbyWithStoredProcs(1);
        db //
                .call("call in1out1(?,?)") //
                .transacted() //
                .in() //
                .out(Type.INTEGER, Integer.class) //
                .input(0, 10, 20) //
                .flatMap(Tx.flattenToValuesOnly()) //
                .test() //
                .awaitDone(TIMEOUT_SECONDS, TimeUnit.SECONDS) //
                .assertValues(0, 10, 20) //
                .assertComplete();
    }

    @Test
    public void testCallableApiReturningTwoOutParameters() throws InterruptedException {
        Database db = DatabaseCreator.createDerbyWithStoredProcs(1);
        db //
                .call("call in1out2(?,?,?)") //
                .in() //
                .out(Type.INTEGER, Integer.class) //
                .out(Type.INTEGER, Integer.class) //
                .input(0, 10, 20) //
                .test() //
                .awaitDone(TIMEOUT_SECONDS, TimeUnit.SECONDS) //
                .assertValueCount(3) //
                .assertValueAt(0, x -> x._1() == 0 && x._2() == 1) //
                .assertValueAt(1, x -> x._1() == 10 && x._2() == 11) //
                .assertValueAt(2, x -> x._1() == 20 && x._2() == 21) //
                .assertComplete();

        db.call("call in1out2(?,?,?)").in().out(Type.INTEGER, Integer.class)
                .out(Type.INTEGER, Integer.class).input(0, 10, 20)
                .blockingForEach(System.out::println);
    }

    @Test
    public void testCallableApiReturningTwoOutParametersTransacted() throws InterruptedException {
        Database db = DatabaseCreator.createDerbyWithStoredProcs(1);
        db //
                .call("call in1out2(?,?,?)") //
                .transacted() //
                .in() //
                .out(Type.INTEGER, Integer.class) //
                .out(Type.INTEGER, Integer.class) //
                .input(0, 10, 20) //
                .flatMap(Tx.flattenToValuesOnly()) //
                .test() //
                .awaitDone(TIMEOUT_SECONDS, TimeUnit.SECONDS) //
                .assertValueCount(3) //
                .assertValueAt(0, x -> x._1() == 0 && x._2() == 1) //
                .assertValueAt(1, x -> x._1() == 10 && x._2() == 11) //
                .assertValueAt(2, x -> x._1() == 20 && x._2() == 21) //
                .assertComplete();

        db.call("call in1out2(?,?,?)") //
                .in() //
                .out(Type.INTEGER, Integer.class) //
                .out(Type.INTEGER, Integer.class) //
                .input(0, 10, 20) //
                .blockingForEach(System.out::println);
    }

    @Test
    public void testCallableApiReturningThreeOutParameters() throws InterruptedException {
        Database db = DatabaseCreator.createDerbyWithStoredProcs(1);
        db //
                .call("call in1out3(?,?,?,?)") //
                .in() //
                .out(Type.INTEGER, Integer.class) //
                .out(Type.INTEGER, Integer.class) //
                .out(Type.INTEGER, Integer.class) //
                .input(0, 10, 20) //
                .test() //
                .awaitDone(TIMEOUT_SECONDS, TimeUnit.SECONDS) //
                .assertValueCount(3) //
                .assertValueAt(0, x -> x._1() == 0 && x._2() == 1 && x._3() == 2) //
                .assertValueAt(1, x -> x._1() == 10 && x._2() == 11 && x._3() == 12) //
                .assertValueAt(2, x -> x._1() == 20 && x._2() == 21 && x._3() == 22) //
                .assertComplete();
    }

    @Test
    public void testCallableApiReturningThreeOutParametersTransacted() throws InterruptedException {
        Database db = DatabaseCreator.createDerbyWithStoredProcs(1);
        db //
                .call("call in1out3(?,?,?,?)") //
                .transacted() //
                .in() //
                .out(Type.INTEGER, Integer.class) //
                .out(Type.INTEGER, Integer.class) //
                .out(Type.INTEGER, Integer.class) //
                .input(0, 10, 20) //
                .flatMap(Tx.flattenToValuesOnly()) //
                .test() //
                .awaitDone(TIMEOUT_SECONDS, TimeUnit.SECONDS) //
                .assertValueCount(3) //
                .assertValueAt(0, x -> x._1() == 0 && x._2() == 1 && x._3() == 2) //
                .assertValueAt(1, x -> x._1() == 10 && x._2() == 11 && x._3() == 12) //
                .assertValueAt(2, x -> x._1() == 20 && x._2() == 21 && x._3() == 22) //
                .assertComplete();
    }

    @Test
    public void testCallableApiReturningOneResultSet() throws InterruptedException {
        Database db = DatabaseCreator.createDerbyWithStoredProcs(1);
        db //
                .call("call in0out0rs1()") //
                .autoMap(Person2.class) //
                .input(0, 10, 20) //
                .doOnNext(x -> {
                    assertTrue(x.outs().isEmpty());
                }) //
                .flatMap(x -> x.results()) //
                .test() //
                .awaitDone(TIMEOUT_SECONDS, TimeUnit.SECONDS) //
                .assertValueAt(0, p -> "FRED".equalsIgnoreCase(p.name()) && p.score() == 24)
                .assertValueAt(1, p -> "SARAH".equalsIgnoreCase(p.name()) && p.score() == 26)
                .assertValueAt(2, p -> "FRED".equalsIgnoreCase(p.name()) && p.score() == 24)
                .assertValueAt(3, p -> "SARAH".equalsIgnoreCase(p.name()) && p.score() == 26)
                .assertValueAt(4, p -> "FRED".equalsIgnoreCase(p.name()) && p.score() == 24)
                .assertValueAt(5, p -> "SARAH".equalsIgnoreCase(p.name()) && p.score() == 26)
                .assertValueCount(6) //
                .assertComplete();
    }

    @Test
    public void testCallableApiReturningOneResultSetTransacted() throws InterruptedException {
        Database db = DatabaseCreator.createDerbyWithStoredProcs(1);
        db //
                .call("call in0out0rs1()") //
                .transacted() //
                .autoMap(Person2.class) //
                .input(0, 10, 20) //
                .flatMap(Tx.flattenToValuesOnly()).doOnNext(x -> {
                    assertTrue(x.outs().isEmpty());
                }) //
                .flatMap(x -> x.results()) //
                .test() //
                .awaitDone(TIMEOUT_SECONDS, TimeUnit.SECONDS) //
                .assertValueAt(0, p -> "FRED".equalsIgnoreCase(p.name()) && p.score() == 24)
                .assertValueAt(1, p -> "SARAH".equalsIgnoreCase(p.name()) && p.score() == 26)
                .assertValueAt(2, p -> "FRED".equalsIgnoreCase(p.name()) && p.score() == 24)
                .assertValueAt(3, p -> "SARAH".equalsIgnoreCase(p.name()) && p.score() == 26)
                .assertValueAt(4, p -> "FRED".equalsIgnoreCase(p.name()) && p.score() == 24)
                .assertValueAt(5, p -> "SARAH".equalsIgnoreCase(p.name()) && p.score() == 26)
                .assertValueCount(6) //
                .assertComplete();
    }

    @Test
    public void testCallableApiReturningTwoResultSetsWithAutoMap() throws InterruptedException {
        Database db = DatabaseCreator.createDerbyWithStoredProcs(1);
        db //
                .call("call in1out0rs2(?)") //
                .in() //
                .autoMap(Person2.class) //
                .autoMap(Person2.class) //
                .input(0, 10, 20) //
                .doOnNext(x -> assertTrue(x.outs().isEmpty())) //
                .flatMap(x -> x.results1().zipWith(x.results2(), (y, z) -> y.name() + z.name())) //
                .test() //
                .awaitDone(TIMEOUT_SECONDS, TimeUnit.SECONDS) //
                .assertValues("FREDSARAH", "SARAHFRED", "FREDSARAH", "SARAHFRED", "FREDSARAH",
                        "SARAHFRED") //
                .assertComplete();
    }

    @Test
    public void testCallableApiReturningTwoResultSetsWithAutoMapTransacted()
            throws InterruptedException {
        Database db = DatabaseCreator.createDerbyWithStoredProcs(1);
        db //
                .call("call in1out0rs2(?)") //
                .transacted() //
                .in() //
                .autoMap(Person2.class) //
                .autoMap(Person2.class) //
                .input(0, 10, 20) //
                .flatMap(Tx.flattenToValuesOnly()) //
                .doOnNext(x -> assertTrue(x.outs().isEmpty())) //
                .flatMap(x -> x.results1().zipWith(x.results2(), (y, z) -> y.name() + z.name())) //
                .test() //
                .awaitDone(TIMEOUT_SECONDS, TimeUnit.SECONDS) //
                .assertValues("FREDSARAH", "SARAHFRED", "FREDSARAH", "SARAHFRED", "FREDSARAH",
                        "SARAHFRED") //
                .assertComplete();
    }

    @Test
    public void testCallableApiReturningTwoResultSetsWithGet() throws InterruptedException {
        Database db = DatabaseCreator.createDerbyWithStoredProcs(1);
        db //
                .call("call in1out0rs2(?)") //
                .in() //
                .getAs(String.class, Integer.class) //
                .getAs(String.class, Integer.class)//
                .input(0, 10, 20) //
                .flatMap(x -> x.results1().zipWith(x.results2(), (y, z) -> y._1() + z._1())) //
                .test() //
                .awaitDone(TIMEOUT_SECONDS, TimeUnit.SECONDS) //
                .assertValues("FREDSARAH", "SARAHFRED", "FREDSARAH", "SARAHFRED", "FREDSARAH",
                        "SARAHFRED") //
                .assertComplete();
    }

    @Test
    public void testCallableApiReturningTwoResultSetsWithGetTransacted()
            throws InterruptedException {
        Database db = DatabaseCreator.createDerbyWithStoredProcs(1);
        db //
                .call("call in1out0rs2(?)") //
                .transacted() //
                .in() //
                .getAs(String.class, Integer.class) //
                .getAs(String.class, Integer.class)//
                .input(0, 10, 20) //
                .flatMap(Tx.flattenToValuesOnly()) //
                .flatMap(x -> x.results1().zipWith(x.results2(), (y, z) -> y._1() + z._1())) //
                .test() //
                .awaitDone(TIMEOUT_SECONDS, TimeUnit.SECONDS) //
                .assertValues("FREDSARAH", "SARAHFRED", "FREDSARAH", "SARAHFRED", "FREDSARAH",
                        "SARAHFRED") //
                .assertComplete();
    }

    @Test
    public void testCallableApiReturningTwoResultSetsSwitchOrder1() throws InterruptedException {
        Database db = DatabaseCreator.createDerbyWithStoredProcs(1);
        db //
                .call("call in1out0rs2(?)") //
                .autoMap(Person2.class) //
                .in() //
                .autoMap(Person2.class) //
                .input(0, 10, 20) //
                .doOnNext(x -> assertTrue(x.outs().isEmpty())) //
                .flatMap(x -> x.results1().zipWith(x.results2(), (y, z) -> y.name() + z.name())) //
                .test() //
                .awaitDone(TIMEOUT_SECONDS, TimeUnit.SECONDS) //
                .assertValues("FREDSARAH", "SARAHFRED", "FREDSARAH", "SARAHFRED", "FREDSARAH",
                        "SARAHFRED") //
                .assertComplete();
    }

    @Test
    public void testCallableApiReturningTwoResultSetsSwitchOrder1Transacted()
            throws InterruptedException {
        Database db = DatabaseCreator.createDerbyWithStoredProcs(1);
        db //
                .call("call in1out0rs2(?)") //
                .transacted() //
                .autoMap(Person2.class) //
                .in() //
                .autoMap(Person2.class) //
                .input(0, 10, 20) //
                .flatMap(Tx.flattenToValuesOnly()) //
                .doOnNext(x -> assertTrue(x.outs().isEmpty())) //
                .flatMap(x -> x.results1().zipWith(x.results2(), (y, z) -> y.name() + z.name())) //
                .test() //
                .awaitDone(TIMEOUT_SECONDS, TimeUnit.SECONDS) //
                .assertValues("FREDSARAH", "SARAHFRED", "FREDSARAH", "SARAHFRED", "FREDSARAH",
                        "SARAHFRED") //
                .assertComplete();
    }

    @Test
    public void testCallableApiReturningTwoResultSetsSwitchOrder2() throws InterruptedException {
        Database db = DatabaseCreator.createDerbyWithStoredProcs(1);
        db //
                .call("call in1out0rs2(?)") //
                .autoMap(Person2.class) //
                .autoMap(Person2.class) //
                .in() //
                .input(0, 10, 20) //
                .doOnNext(x -> assertTrue(x.outs().isEmpty())) //
                .flatMap(x -> x.results1().zipWith(x.results2(), (y, z) -> y.name() + z.name())) //
                .test() //
                .awaitDone(TIMEOUT_SECONDS, TimeUnit.SECONDS) //
                .assertValues("FREDSARAH", "SARAHFRED", "FREDSARAH", "SARAHFRED", "FREDSARAH",
                        "SARAHFRED") //
                .assertComplete();
    }

    @Test
    public void testCallableApiReturningTwoResultSetsSwitchOrder2Transacted()
            throws InterruptedException {
        Database db = DatabaseCreator.createDerbyWithStoredProcs(1);
        db //
                .call("call in1out0rs2(?)") //
                .transacted() //
                .autoMap(Person2.class) //
                .autoMap(Person2.class) //
                .in() //
                .input(0, 10, 20) //
                .flatMap(Tx.flattenToValuesOnly()) //
                .doOnNext(x -> assertTrue(x.outs().isEmpty())) //
                .flatMap(x -> x.results1().zipWith(x.results2(), (y, z) -> y.name() + z.name())) //
                .test() //
                .awaitDone(TIMEOUT_SECONDS, TimeUnit.SECONDS) //
                .assertValues("FREDSARAH", "SARAHFRED", "FREDSARAH", "SARAHFRED", "FREDSARAH",
                        "SARAHFRED") //
                .assertComplete();
    }

    @Test
    public void testCallableApiReturningTwoOutputThreeResultSets() throws InterruptedException {
        Database db = DatabaseCreator.createDerbyWithStoredProcs(1);
        db //
                .call("call in0out2rs3(?, ?)") //
                .out(Type.INTEGER, Integer.class) //
                .out(Type.INTEGER, Integer.class) //
                .autoMap(Person2.class) //
                .autoMap(Person2.class) //
                .autoMap(Person2.class) //
                .input(0, 10, 20) //
                .doOnNext(x -> {
                    assertEquals(2, x.outs().size());
                    assertEquals(1, x.outs().get(0));
                    assertEquals(2, x.outs().get(1));
                }) //
                .flatMap(x -> x.results1().zipWith(x.results2(), (y, z) -> y.name() + z.name())
                        .zipWith(x.results3(), (y, z) -> y + z.name())) //
                .test() //
                .awaitDone(TIMEOUT_SECONDS, TimeUnit.SECONDS) //
                .assertNoErrors() //
                .assertValues("FREDSARAHFRED", "SARAHFREDSARAH", "FREDSARAHFRED", "SARAHFREDSARAH",
                        "FREDSARAHFRED", "SARAHFREDSARAH") //
                .assertComplete();
    }

    @Test
    public void testCallableApiReturningTwoOutputThreeResultSetsTransacted()
            throws InterruptedException {
        Database db = DatabaseCreator.createDerbyWithStoredProcs(1);
        db //
                .call("call in0out2rs3(?, ?)") //
                .transacted() //
                .out(Type.INTEGER, Integer.class) //
                .out(Type.INTEGER, Integer.class) //
                .autoMap(Person2.class) //
                .autoMap(Person2.class) //
                .autoMap(Person2.class) //
                .input(0, 10, 20) //
                .flatMap(Tx.flattenToValuesOnly()) //
                .doOnNext(x -> {
                    assertEquals(2, x.outs().size());
                    assertEquals(1, x.outs().get(0));
                    assertEquals(2, x.outs().get(1));
                }) //
                .flatMap(x -> x.results1().zipWith(x.results2(), (y, z) -> y.name() + z.name())
                        .zipWith(x.results3(), (y, z) -> y + z.name())) //
                .test() //
                .awaitDone(TIMEOUT_SECONDS, TimeUnit.SECONDS) //
                .assertNoErrors() //
                .assertValues("FREDSARAHFRED", "SARAHFREDSARAH", "FREDSARAHFRED", "SARAHFREDSARAH",
                        "FREDSARAHFRED", "SARAHFREDSARAH") //
                .assertComplete();
    }

    @Test
    public void testCallableApiReturningTenOutParameters() {
        Database db = DatabaseCreator.createDerbyWithStoredProcs(1);
        db //
                .call("call out10(?,?,?,?,?,?,?,?,?,?)") //
                .out(Type.INTEGER, Integer.class) //
                .out(Type.INTEGER, Integer.class) //
                .out(Type.INTEGER, Integer.class) //
                .out(Type.INTEGER, Integer.class) //
                .out(Type.INTEGER, Integer.class) //
                .out(Type.INTEGER, Integer.class) //
                .out(Type.INTEGER, Integer.class) //
                .out(Type.INTEGER, Integer.class) //
                .out(Type.INTEGER, Integer.class) //
                .out(Type.INTEGER, Integer.class) //
                .input(0, 10) //
                .test() //
                .awaitDone(TIMEOUT_SECONDS, TimeUnit.SECONDS) //
                .assertNoErrors() //
                .assertValueCount(2) //
                .assertComplete();
    }

    @Test
    public void testCallableApiReturningTenOutParametersTransacted() {
        Database db = DatabaseCreator.createDerbyWithStoredProcs(1);
        db //
                .call("call out10(?,?,?,?,?,?,?,?,?,?)") //
                .transacted() //
                .out(Type.INTEGER, Integer.class) //
                .out(Type.INTEGER, Integer.class) //
                .out(Type.INTEGER, Integer.class) //
                .out(Type.INTEGER, Integer.class) //
                .out(Type.INTEGER, Integer.class) //
                .out(Type.INTEGER, Integer.class) //
                .out(Type.INTEGER, Integer.class) //
                .out(Type.INTEGER, Integer.class) //
                .out(Type.INTEGER, Integer.class) //
                .out(Type.INTEGER, Integer.class) //
                .input(0, 10) //
                .flatMap(Tx.flattenToValuesOnly()) //
                .test() //
                .awaitDone(TIMEOUT_SECONDS, TimeUnit.SECONDS) //
                .assertNoErrors() //
                .assertValueCount(2) //
                .assertComplete();
    }

    @Test
    public void testCallableApiReturningTenResultSets() {
        Database db = DatabaseCreator.createDerbyWithStoredProcs(1);
        db //
                .call("call rs10()") //
                .autoMap(Person2.class) //
                .autoMap(Person2.class) //
                .autoMap(Person2.class) //
                .autoMap(Person2.class) //
                .autoMap(Person2.class) //
                .autoMap(Person2.class) //
                .autoMap(Person2.class) //
                .autoMap(Person2.class) //
                .autoMap(Person2.class) //
                .autoMap(Person2.class) //
                .input(0, 10) //
                .doOnNext(x -> {
                    assertEquals(0, x.outs().size());
                    assertEquals(10, x.results().size());
                }) //
                   // just zip the first and last result sets
                .flatMap(x -> x.results(0) //
                        .zipWith(x.results(9), //
                                (y, z) -> ((Person2) y).name() + ((Person2) z).name()))
                .test() //
                .awaitDone(TIMEOUT_SECONDS, TimeUnit.SECONDS) //
                .assertNoErrors() //
                .assertValues("FREDSARAH", "SARAHFRED", "FREDSARAH", "SARAHFRED") //
                .assertComplete();
    }

    @Test
    public void testCallableApiReturningTenResultSetsTransacted() {
        Database db = DatabaseCreator.createDerbyWithStoredProcs(1);
        db //
                .call("call rs10()") //
                .transacted() //
                .autoMap(Person2.class) //
                .autoMap(Person2.class) //
                .autoMap(Person2.class) //
                .autoMap(Person2.class) //
                .autoMap(Person2.class) //
                .autoMap(Person2.class) //
                .autoMap(Person2.class) //
                .autoMap(Person2.class) //
                .autoMap(Person2.class) //
                .autoMap(Person2.class) //
                .input(0, 10) //
                .flatMap(Tx.flattenToValuesOnly()) //
                .doOnNext(x -> {
                    assertEquals(0, x.outs().size());
                    assertEquals(10, x.results().size());
                }) //
                   // just zip the first and last result sets
                .flatMap(x -> x.results(0) //
                        .zipWith(x.results(9), //
                                (y, z) -> ((Person2) y).name() + ((Person2) z).name()))
                .test() //
                .awaitDone(TIMEOUT_SECONDS, TimeUnit.SECONDS) //
                .assertNoErrors() //
                .assertValues("FREDSARAH", "SARAHFRED", "FREDSARAH", "SARAHFRED") //
                .assertComplete();
    }

    @Test
    public void testCallableApiReturningOneResultSetGetAs() throws InterruptedException {
        Database db = DatabaseCreator.createDerbyWithStoredProcs(1);
        db //
                .call("call in0out0rs1()") //
                .getAs(String.class, Integer.class) //
                .input(1) //
                .flatMap(x -> x.results()) //
                .test() //
                .awaitDone(TIMEOUT_SECONDS, TimeUnit.SECONDS) //
                .assertValueAt(0, p -> "FRED".equalsIgnoreCase(p._1()) && p._2() == 24) //
                .assertValueAt(1, p -> "SARAH".equalsIgnoreCase(p._1()) && p._2() == 26) //
                .assertComplete();
    }

    @Test
    public void testH2InClauseWithoutSetArray() {
        db().apply(con -> {
            try (PreparedStatement ps = con
                    .prepareStatement("select count(*) from person where name in (?, ?)")) {
                ps.setString(1, "FRED");
                ps.setString(2, "JOSEPH");
                ResultSet rs = ps.executeQuery();
                rs.next();
                return rs.getInt(1);
            }
        }).test() //
                .awaitDone(TIMEOUT_SECONDS, TimeUnit.SECONDS).assertComplete() // ;
                .assertValue(2); //
    }

    @Test
    @Ignore
    public void testH2InClauseWithSetArray() {
        db().apply(con -> {
            try (PreparedStatement ps = con
                    .prepareStatement("select count(*) from person where name in (?)")) {
                ps.setArray(1, con.createArrayOf("VARCHAR", new String[] { "FRED", "JOSEPH" }));
                ResultSet rs = ps.executeQuery();
                rs.next();
                return rs.getInt(1);
            }
        }).test() //
                .awaitDone(TIMEOUT_SECONDS, TimeUnit.SECONDS).assertComplete() // ;
                .assertValue(2); //
    }

    @Test
    public void testUpdateTxPerformed() {
        Database db = db(1);
        db.update("update person set score = 1000") //
                .transacted() //
                .counts() //
                .test() //
                .awaitDone(TIMEOUT_SECONDS, TimeUnit.SECONDS) //
                .assertComplete() //
                .assertValueCount(2); // value and complete

        db.select("select count(*) from person where score=1000") //
                .getAs(Integer.class) //
                .test() //
                .awaitDone(TIMEOUT_SECONDS, TimeUnit.SECONDS) //
                .assertComplete() //
                .assertValue(3);

    }

    @Test
    public void testIssue20AutoCommitEnabledAndConnectionThrowsOnCommit() {
        ConnectionProvider cp = DatabaseCreator.connectionProvider();
        Database db = Database.fromBlocking(new ConnectionProvider() {

            @Override
            public Connection get() {
                Connection c = cp.get();
                try {
                    c.setAutoCommit(true);
                } catch (SQLException e) {
                    throw new SQLRuntimeException(e);
                }
                return new DelegatedConnection() {

                    @Override
                    public Connection con() {
                        return c;
                    }

                    @Override
                    public void commit() throws SQLException {
                        System.out.println("COMMITTING");
                        if (this.getAutoCommit()) {
                            throw new SQLException("cannot commit when autoCommit is true");
                        } else {
                            con().commit();
                        }
                    }

                };
            }

            @Override
            public void close() {
                // do nothing
            }
        });
        db.update("insert into note(text) values(?)") //
                .parameters("HI", "THERE") //
                .returnGeneratedKeys() //
                .getAs(Integer.class)//
                .test() //
                .awaitDone(TIMEOUT_SECONDS, TimeUnit.SECONDS) //
                .assertValues(1, 2) //
                .assertComplete();
    }

    private static final class Plugins {

        private static final List<Throwable> list = new CopyOnWriteArrayList<Throwable>();

        static void reset() {
            list.clear();
            RxJavaPlugins.setErrorHandler(e -> list.add(e));
        }

        static Throwable getSingleError() {
            assertEquals(1, list.size());
            RxJavaPlugins.reset();
            return list.get(0);
        }
    }

    @Test
    public void testIssue27ConnectionErrorReportedToRxJavaPlugins() {
        try (Database db = Database.nonBlocking()
                .url("jdbc:driverdoesnotexist://doesnotexist:1527/notThere").build()) {
            Plugins.reset();
            db.select("select count(*) from person") //
                    .getAs(Long.class) //
                    .test() //
                    .awaitDone(TIMEOUT_SECONDS / 5, TimeUnit.SECONDS) //
                    .assertNoValues() //
                    .assertNotTerminated() //
                    .cancel();
            Throwable e = Plugins.getSingleError();
            assertTrue(e instanceof UndeliverableException);
            assertTrue(e.getMessage().toLowerCase().contains("no suitable driver"));
        }
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
