package org.davidmoten.rx.jdbc.pool;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.davidmoten.rx.jdbc.Database;
import org.davidmoten.rx.pool.Consumers;
import org.davidmoten.rx.pool.Member;
import org.davidmoten.rx.pool.NonBlockingPool;
import org.davidmoten.rx.pool.Pool;
import org.junit.Test;

import io.reactivex.Flowable;
import io.reactivex.schedulers.TestScheduler;
import io.reactivex.subscribers.TestSubscriber;

public class PoolTest {

    @Test
    public void testMaxIdleTime() throws InterruptedException {
        TestScheduler s = new TestScheduler();
        AtomicInteger count = new AtomicInteger();
        AtomicInteger disposed = new AtomicInteger();
        Pool<Integer> pool = NonBlockingPool //
                .factory(() -> count.incrementAndGet()) //
                .healthy(n -> true) //
                .disposer(Consumers.doNothing()) //
                .maxSize(3) //
                .maxIdleTime(1, TimeUnit.MINUTES) //
                .returnToPoolDelayAfterHealthCheckFailure(1, TimeUnit.SECONDS) //
                .disposer(n -> disposed.incrementAndGet()) //
                .scheduler(s) //
                .build();
        TestSubscriber<Member<Integer>> ts = pool //
                .member() //
                .repeat() //
                .doOnNext(m -> m.checkin()) //
                .doOnNext(System.out::println) //
                .doOnRequest(t -> System.out.println("test request=" + t)) //
                .test(1);
        s.triggerActions();
        ts.assertValueCount(1);
        assertEquals(0, disposed.get());
        s.advanceTimeBy(1, TimeUnit.MINUTES);
        s.triggerActions();
        assertEquals(1, disposed.get());
    }

    @Test
    public void testReleasedMemberIsRecreated() throws InterruptedException {
        TestScheduler s = new TestScheduler();
        AtomicInteger count = new AtomicInteger();
        AtomicInteger disposed = new AtomicInteger();
        Pool<Integer> pool = NonBlockingPool //
                .factory(() -> count.incrementAndGet()) //
                .healthy(n -> true) //
                .disposer(Consumers.doNothing()) //
                .maxSize(1) //
                .maxIdleTime(1, TimeUnit.MINUTES) //
                .returnToPoolDelayAfterHealthCheckFailure(1, TimeUnit.SECONDS) //
                .disposer(n -> disposed.incrementAndGet()) //
                .scheduler(s) //
                .build();
        {
            TestSubscriber<Member<Integer>> ts = pool //
                    .member() //
                    .repeat() //
                    .doOnNext(m -> m.checkin()) //
                    .doOnNext(System.out::println) //
                    .doOnRequest(t -> System.out.println("test request=" + t)) //
                    .test(1);
            s.triggerActions();
            ts.assertValueCount(1);
            assertEquals(0, disposed.get());
            s.advanceTimeBy(1, TimeUnit.MINUTES);
            s.triggerActions();
            assertEquals(1, disposed.get());
            ts.cancel();
            assertEquals(1, disposed.get());
        }
        {
            TestSubscriber<Member<Integer>> ts = pool //
                    .member() //
                    .repeat() //
                    .doOnNext(m -> m.checkin()) //
                    .doOnNext(System.out::println) //
                    .doOnRequest(t -> System.out.println("test request=" + t)) //
                    .test(1);
            s.triggerActions();
            ts.assertValueCount(1);
            assertEquals(1, disposed.get());
            s.advanceTimeBy(1, TimeUnit.MINUTES);
            s.triggerActions();
            assertEquals(2, disposed.get());
        }

    }

    @Test
    public void testDirectSchedule() {
        TestScheduler s = new TestScheduler();
        AtomicBoolean b = new AtomicBoolean();
        s.scheduleDirect(() -> b.set(true), 1, TimeUnit.MINUTES);
        s.scheduleDirect(() -> b.set(false), 2, TimeUnit.MINUTES);
        s.advanceTimeBy(1, TimeUnit.MINUTES);
        assertTrue(b.get());
        s.advanceTimeBy(1, TimeUnit.MINUTES);
        assertFalse(b.get());
    }

    @Test
    public void testConnectionPoolRecylesAlternating() {
        TestScheduler s = new TestScheduler();
        Database db = DatabaseCreator.create(2, s);
        TestSubscriber<Connection> ts = db.connection() //
                .repeat() //
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
        assertTrue(list.get(0).hashCode() == list.get(1).hashCode());
        assertTrue(list.get(1).hashCode() == list.get(2).hashCode());
        assertTrue(list.get(2).hashCode() == list.get(3).hashCode());
    }

    @Test
    public void testFlowableFromIterable() {
        Flowable.fromIterable(Arrays.asList(1, 2)).test(4).assertValues(1, 2);
    }

    @Test
    public void testConnectionPoolRecylesMany() throws SQLException {
        TestScheduler s = new TestScheduler();
        Database db = DatabaseCreator.create(2, s);
        TestSubscriber<Connection> ts = db //
                .connection() //
                .repeat() //
                .test(4); //
        s.triggerActions();
        ts.assertNoErrors() //
                .assertValueCount(2) //
                .assertNotTerminated();
        List<Connection> list = new ArrayList<>(ts.values());
        list.get(1).close(); // should release a connection
        s.triggerActions();
        {
            List<Object> values = ts.assertValueCount(3) //
                    .assertNotTerminated() //
                    .getEvents().get(0);
            assertEquals(list.get(0).hashCode(), values.get(0).hashCode());
            assertEquals(list.get(1).hashCode(), values.get(1).hashCode());
            assertEquals(list.get(1).hashCode(), values.get(2).hashCode());
        }
        // .assertValues(list.get(0), list.get(1), list.get(1));
        list.get(0).close();
        s.triggerActions();

        {
            List<Object> values = ts.assertValueCount(4) //
                    .assertNotTerminated() //
                    .getEvents().get(0);
            assertEquals(list.get(0).hashCode(), values.get(0).hashCode());
            assertEquals(list.get(1).hashCode(), values.get(1).hashCode());
            assertEquals(list.get(1).hashCode(), values.get(2).hashCode());
            assertEquals(list.get(0).hashCode(), values.get(3).hashCode());
        }
    }

}
