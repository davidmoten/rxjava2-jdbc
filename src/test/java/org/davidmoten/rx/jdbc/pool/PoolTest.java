package org.davidmoten.rx.jdbc.pool;

import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.davidmoten.rx.jdbc.Database;
import org.davidmoten.rx.pool.Member;
import org.davidmoten.rx.pool.MemberFactory;
import org.davidmoten.rx.pool.NonBlockingMember;
import org.davidmoten.rx.pool.NonBlockingPool;
import org.davidmoten.rx.pool.Pool;
import org.junit.Test;

import io.reactivex.Flowable;
import io.reactivex.schedulers.TestScheduler;
import io.reactivex.subscribers.TestSubscriber;

public class PoolTest {

    @Test
    public void testSimplePool() throws InterruptedException {
        TestScheduler s = new TestScheduler();
        AtomicInteger count = new AtomicInteger();
        MemberFactory<Integer, NonBlockingPool<Integer>> memberFactory = pool -> new NonBlockingMember<Integer>(pool,
                null);
        Pool<Integer> pool = NonBlockingPool.factory(() -> count.incrementAndGet()) //
                .healthy(n -> true) //
                .disposer(n -> {
                }) //
                .maxSize(3)//
                .returnToPoolDelayAfterHealthCheckFailureMs(1000)//
                .memberFactory(memberFactory)//
                .scheduler(s)//
                .build();
        TestSubscriber<Member<Integer>> ts = pool.members() //
                .doOnNext(m -> m.checkin()) //
                .doOnNext(System.out::println) //
                .test(4);
        s.triggerActions();
        ts.assertValueCount(4);
    }

    @Test
    public void testConnectionPoolRecylesAlternating() {
        TestScheduler s = new TestScheduler();
        Database db = DatabaseCreator.create(2, s);
        TestSubscriber<Connection> ts = db.connections() //
                .doOnNext(System.out::println) //
                .doOnNext(c -> {
                    // release connection for reuse straight away
                    c.close();
                }) //
                .test(4); //
        s.triggerActions();
        ts.assertValueCount(4) //
                .assertNotTerminated();
        List<Object> list = ts.getEvents().get(0);
        // all 4 connections released were the same
        System.out.println(list);
        assertTrue(list.get(0) == list.get(2));
        assertTrue(list.get(1) == list.get(3));
        assertTrue(list.get(0) != list.get(1));
    }

    @Test
    public void testFlowableFromIterable() {
        Flowable.fromIterable(Arrays.asList(1, 2)).test(4).assertValues(1, 2);
    }

    @Test
    public void testConnectionPoolRecylesMany() throws SQLException {
        TestScheduler s = new TestScheduler();
        Database db = DatabaseCreator.create(2, s);
        TestSubscriber<Connection> ts = db.connections() //
                .test(4); //
        s.triggerActions();
        ts.assertNoErrors() //
                .assertValueCount(2) //
                .assertNotTerminated();
        List<Connection> list = new ArrayList<>(ts.values());
        list.get(1).close(); // should release a connection
        s.triggerActions();
        ts.assertValueCount(3) //
                .assertNotTerminated() //
                .assertValues(list.get(0), list.get(1), list.get(1));
        list.get(0).close();
        s.triggerActions();
        ts.assertValues(list.get(0), list.get(1), list.get(1), list.get(0)) //
                .assertValueCount(4) //
                .assertNotTerminated();
    }

}
