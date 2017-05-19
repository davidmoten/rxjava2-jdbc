package org.davidmoten.rx;

import java.util.List;

import org.davidmoten.rx.jdbc.Database;
import org.davidmoten.rx.jdbc.annotations.Column;
import org.davidmoten.rx.jdbc.pool.DatabaseCreator;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.davidmoten.guavamini.Lists;

import io.reactivex.Flowable;

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
        List<String> list = Lists.newArrayList("hello", "there");
        List<Flowable<List<String>>> list2 = Lists
                .newArrayList(Flowable.fromIterable(list).buffer(1));
        Flowable.concat(list2).doOnNext(System.out::println).subscribe();
        db() //
                .select("select score from person where name=?") //
                .parameters("FRED", "JOSEPH") //
                .getAs(Integer.class) //
                .test() //
                .assertNoErrors() //
                .assertValues(21, 34) //
                .assertComplete();
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

    @Test(expected = IllegalArgumentException.class)
    public void testSelectParametersSpecifiedWhenNoneExpectedThrowsImmediately() {
        db() //
                .select("select score from person") //
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
        // we should be able to do this with 2 connections only
        // using tx() we get to resuse current connection
        Database db = db(2);
        db.select("select score from person where name=?") //
                .parameters("FRED", "JOSEPH") //
                .getAs(Integer.class) //
                .concatMap(score -> {
                    log.info("score={}", score);
                    return db //
                            .select("select name from person where score = ?") //
                            .parameters(score) //
                            .getAs(String.class)
                            .doOnComplete(() -> log.info("completed select where score=" + score));
                }) //
                .test() //
                .assertNoErrors() //
                .assertValues("FRED", "JOSEPH") //
                .assertComplete(); //
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
    public void testSelectWithoutWhereClause() {
        Assert.assertEquals(3, (long) db().select("select name from person") //
                .count().blockingGet());
    }

    static interface Person {
        @Column
        String name();
    }

    public static void main(String[] args) {
        Flowable.just(1, 2, 3, 4, 5, 6).doOnRequest(System.out::println).subscribe();
    }

}
