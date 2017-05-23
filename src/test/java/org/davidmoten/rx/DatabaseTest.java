package org.davidmoten.rx;

import static org.junit.Assert.assertEquals;

import java.sql.SQLSyntaxErrorException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.davidmoten.rx.jdbc.Database;
import org.davidmoten.rx.jdbc.Pools;
import org.davidmoten.rx.jdbc.annotations.Column;
import org.davidmoten.rx.jdbc.annotations.Index;
import org.davidmoten.rx.jdbc.exceptions.AnnotationsNotFoundException;
import org.davidmoten.rx.jdbc.exceptions.ColumnIndexOutOfRangeException;
import org.davidmoten.rx.jdbc.exceptions.ColumnNotFoundException;
import org.davidmoten.rx.jdbc.pool.DatabaseCreator;
import org.davidmoten.rx.jdbc.pool.NonBlockingConnectionPool;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.reactivex.Flowable;
import io.reactivex.schedulers.Schedulers;

public class DatabaseTest {

    private static final Logger log = LoggerFactory.getLogger(DatabaseTest.class);

    private static Database db() {
        return DatabaseCreator.create(1);
    }

    private static Database db(int poolSize) {
        return DatabaseCreator.create(poolSize);
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
                .parameterListStream(
                        Flowable.just(Arrays.asList("FRED", 21), Arrays.asList("JOSEPH", 34))) //
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
                .doOnNext(System.out::println) //
                .test() //
                .assertValueCount(3) //
                .assertComplete();
    }

    @Test
    public void testSelectTransactedChained() throws Exception {
        System.out.println("testSelectTransactedChained");
        Database db = db();
        db //
                .select("select score from person where name=?") //
                .parameters("FRED", "JOSEPH") //
                .transacted() //
                .transactedValuesOnly() //
                .getAs(Integer.class) //
                .doOnNext(System.out::println)//
                .flatMap(tx -> db //
                        .tx(tx) //
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
    public void testReadMeFragment1() {
        Database db = Database.test();
        db.select("select name from person").getAs(String.class).forEach(System.out::println);
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
    public void testTupleSupport() {
        db().select("select name, score from person").getAs(String.class, Integer.class)
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
        latch.await(5, TimeUnit.SECONDS);
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
    public void testSelectWithoutWhereClause() {
        Assert.assertEquals(3, (long) db().select("select name from person") //
                .count().blockingGet());
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

    public static void main(String[] args) {
        Flowable.just(1, 2, 3, 4, 5, 6).doOnRequest(System.out::println).subscribe();
    }

}
