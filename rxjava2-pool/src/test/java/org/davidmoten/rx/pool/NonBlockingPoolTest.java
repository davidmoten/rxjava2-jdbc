package org.davidmoten.rx.pool;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.davidmoten.rx.internal.FlowableSingleDeferUntilRequest;
import org.junit.Test;

import io.reactivex.Flowable;
import io.reactivex.Scheduler;
import io.reactivex.Single;
import io.reactivex.SingleObserver;
import io.reactivex.disposables.Disposable;
import io.reactivex.exceptions.UndeliverableException;
import io.reactivex.functions.Consumer;
import io.reactivex.observers.TestObserver;
import io.reactivex.plugins.RxJavaPlugins;
import io.reactivex.schedulers.Schedulers;
import io.reactivex.schedulers.TestScheduler;
import io.reactivex.subscribers.TestSubscriber;

public class NonBlockingPoolTest {

    @Test
    public void testMaxIdleTime() throws InterruptedException {
        TestScheduler s = new TestScheduler();
        AtomicInteger count = new AtomicInteger();
        AtomicInteger disposed = new AtomicInteger();
        Pool<Integer> pool = NonBlockingPool //
                .factory(() -> count.incrementAndGet()) //
                .healthCheck(n -> true) //
                .maxSize(3) //
                .maxIdleTime(1, TimeUnit.MINUTES) //
                .disposer(n -> disposed.incrementAndGet()) //
                .scheduler(s) //
                .build();
        TestSubscriber<Member<Integer>> ts = new FlowableSingleDeferUntilRequest<>( //
                pool.member()) //
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
    public void testMaxIdleTimeResetIfUsed() throws InterruptedException {
        TestScheduler s = new TestScheduler();
        AtomicInteger count = new AtomicInteger();
        AtomicInteger disposed = new AtomicInteger();
        Pool<Integer> pool = NonBlockingPool //
                .factory(() -> count.incrementAndGet()) //
                .healthCheck(n -> true) //
                .maxSize(1) //
                .maxIdleTime(2, TimeUnit.MINUTES) //
                .disposer(n -> disposed.incrementAndGet()) //
                .scheduler(s) //
                .build();
        Single<Member<Integer>> member = pool.member() //
                .doOnSuccess(System.out::println) //
                .doOnSuccess(m -> m.checkin());
        member.subscribe();
        s.triggerActions();
        assertEquals(0, disposed.get());
        s.advanceTimeBy(1, TimeUnit.MINUTES);
        s.triggerActions();
        member.subscribe();
        s.advanceTimeBy(1, TimeUnit.MINUTES);
        s.triggerActions();
        assertEquals(0, disposed.get());
        s.advanceTimeBy(1, TimeUnit.MINUTES);
        s.triggerActions();
        assertEquals(1, disposed.get());
    }

    @Test
    public void testReleasedMemberIsRecreated() throws Exception {
        TestScheduler s = new TestScheduler();
        AtomicInteger count = new AtomicInteger();
        AtomicInteger disposed = new AtomicInteger();
        Pool<Integer> pool = NonBlockingPool //
                .factory(() -> count.incrementAndGet()) //
                .healthCheck(n -> true) //
                .maxSize(1) //
                .maxIdleTime(1, TimeUnit.MINUTES) //
                .disposer(n -> disposed.incrementAndGet()) //
                .scheduler(s) //
                .build();
        {
            TestSubscriber<Member<Integer>> ts = new FlowableSingleDeferUntilRequest<>(pool //
                    .member()) //
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
        // check Pool.close() disposes value
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
            assertEquals(2, disposed.get());
        }
        pool.close();
        assertEquals(3, disposed.get());
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
        AtomicInteger count = new AtomicInteger();
        Pool<Integer> pool = NonBlockingPool //
                .factory(() -> count.incrementAndGet()) //
                .healthCheck(n -> true) //
                .maxSize(2) //
                .maxIdleTime(1, TimeUnit.MINUTES) //
                .scheduler(s) //
                .build();
        TestSubscriber<Integer> ts = new FlowableSingleDeferUntilRequest<>(pool.member()) //
                .repeat() //
                .doOnNext(m -> m.checkin()) //
                .map(m -> m.value()) //
                .test(4); //
        s.triggerActions();
        ts.assertValueCount(4) //
                .assertNotTerminated();
        List<Object> list = ts.getEvents().get(0);
        // all 4 connections released were the same
        assertTrue(list.get(0) == list.get(1));
        assertTrue(list.get(1) == list.get(2));
        assertTrue(list.get(2) == list.get(3));
    }

    @Test
    public void testFlowableFromIterable() {
        Flowable.fromIterable(Arrays.asList(1, 2)).test(4).assertValues(1, 2);
    }

    @Test
    public void testConnectionPoolRecylesMany() throws SQLException {
        TestScheduler s = new TestScheduler();
        AtomicInteger count = new AtomicInteger();
        Pool<Integer> pool = NonBlockingPool //
                .factory(() -> count.incrementAndGet()) //
                .healthCheck(n -> true) //
                .maxSize(2) //
                .maxIdleTime(1, TimeUnit.MINUTES) //
                .scheduler(s) //
                .build();
        TestSubscriber<Member<Integer>> ts = new FlowableSingleDeferUntilRequest<>(pool.member()) //
                .repeat() //
                .test(4); //
        s.triggerActions();
        ts.assertNoErrors() //
                .assertValueCount(2) //
                .assertNotTerminated();
        List<Member<Integer>> list = new ArrayList<>(ts.values());
        list.get(1).checkin(); // should release a connection
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
        list.get(0).checkin();
        s.triggerActions();

        {
            List<Object> values = ts.assertValueCount(4) //
                    .assertNotTerminated() //
                    .getEvents().get(0);
            assertEquals(list.get(0), values.get(0));
            assertEquals(list.get(1), values.get(1));
            assertEquals(list.get(1), values.get(2));
            assertEquals(list.get(0), values.get(3));
        }
    }

    @Test
    public void testConnectionPoolRecylesLastInFirstOut() throws SQLException {
        AtomicInteger count = new AtomicInteger();
        Pool<Integer> pool = NonBlockingPool //
                .factory(() -> count.incrementAndGet()) //
                .healthCheck(n -> true) //
                .maxSize(4) //
                .maxIdleTime(1, TimeUnit.MINUTES) //
                .build();
        Member<Integer> m1 = pool.member().blockingGet();
        Member<Integer> m2 = pool.member().blockingGet();
        m1.checkin();
        m2.checkin();
        Member<Integer> m3 = pool.member().blockingGet();
        assertTrue(m2 == m3);
    }
    
    @Test
    public void testHealthCheckWhenFails() throws Exception {
        TestScheduler s = new TestScheduler();
        AtomicInteger count = new AtomicInteger();
        AtomicInteger disposed = new AtomicInteger();
        AtomicInteger healthChecks = new AtomicInteger();
        Pool<Integer> pool = NonBlockingPool //
                .factory(() -> count.incrementAndGet()) //
                .healthCheck(n -> {
                    healthChecks.incrementAndGet();
                    return false;
                }) //
                .createRetryInterval(10, TimeUnit.MINUTES) //
                .idleTimeBeforeHealthCheck(1, TimeUnit.MILLISECONDS) //
                .maxSize(1) //
                .maxIdleTime(1, TimeUnit.HOURS) //
                .disposer(n -> disposed.incrementAndGet()) //
                .scheduler(s) //
                .build();
        {
            TestSubscriber<Member<Integer>> ts = new FlowableSingleDeferUntilRequest<>(pool.member()) //
                    .repeat() //
                    .doOnNext(System.out::println) //
                    .doOnNext(m -> m.checkin()) //
                    .doOnRequest(t -> System.out.println("test request=" + t)) //
                    .test(1);
            s.triggerActions();
            // health check doesn't get run on create
            ts.assertValueCount(1);
            assertEquals(0, disposed.get());
            assertEquals(0, healthChecks.get());
            // next request is immediate so health check does not run
            System.out.println("health check should not run because immediate");
            ts.request(1);
            s.triggerActions();
            ts.assertValueCount(2);
            assertEquals(0, disposed.get());
            assertEquals(0, healthChecks.get());

            // now try to trigger health check
            s.advanceTimeBy(1, TimeUnit.MILLISECONDS);
            s.triggerActions();
            System.out.println("trying to trigger health check");
            ts.request(1);
            s.triggerActions();
            ts.assertValueCount(2);
            assertEquals(1, disposed.get());
            assertEquals(1, healthChecks.get());

            // checkout retry should happen after interval
            s.advanceTimeBy(10, TimeUnit.MINUTES);
            ts.assertValueCount(3);

            // failing health check causes recreate to be scheduled
            ts.cancel();
            // already disposed so cancel has no effect
            assertEquals(1, disposed.get());
        }
    }

    @Test
    public void testMemberAvailableAfterCreationScheduledIsUsedImmediately() throws InterruptedException {
        TestScheduler ts = new TestScheduler();
        Scheduler s = createScheduleToDelayCreation(ts);
        AtomicInteger count = new AtomicInteger();
        Pool<Integer> pool = NonBlockingPool //
                .factory(() -> count.incrementAndGet()) //
                .createRetryInterval(10, TimeUnit.MINUTES) //
                .maxSize(2) //
                .maxIdleTime(1, TimeUnit.HOURS) //
                .scheduler(s) //
                .build();
        List<Member<Integer>> list = new ArrayList<Member<Integer>>();
        pool.member().doOnSuccess(m -> list.add(m)).subscribe();
        assertEquals(0, list.size());
        ts.advanceTimeBy(1, TimeUnit.MINUTES);
        ts.triggerActions();
        assertEquals(1, list.size());
        pool.member().doOnSuccess(m -> list.add(m)).subscribe();
        list.get(0).checkin();
        ts.triggerActions();
        assertEquals(2, list.size());
    }

    public static class TestException extends Exception {

        private static final long serialVersionUID = 4243235711346034313L;

    }

    @Test
    public void testPoolFactoryWhenFailsThenRecovers() {
        AtomicReference<Throwable> ex = new AtomicReference<>();
        Consumer<? super Throwable> handler = RxJavaPlugins.getErrorHandler();
        RxJavaPlugins.setErrorHandler(t -> ex.set(t));
        try {
            TestScheduler s = new TestScheduler();
            AtomicInteger c = new AtomicInteger();
            NonBlockingPool<Integer> pool = NonBlockingPool.factory(() -> {
                if (c.getAndIncrement() == 0) {
                    throw new TestException();
                } else {
                    return c.get();
                }
            }) //
                    .maxSize(1) //
                    .scheduler(s) //
                    .createRetryInterval(10, TimeUnit.SECONDS) //
                    .build();
            TestObserver<Integer> ts = pool.member() //
                    .map(m -> m.value()) //
                    .test() //
                    .assertNotTerminated() //
                    .assertNoValues();
            s.triggerActions();
            assertTrue(ex.get() instanceof UndeliverableException);
            assertTrue(((UndeliverableException) ex.get()).getCause() instanceof TestException);
            s.advanceTimeBy(10, TimeUnit.SECONDS);
            s.triggerActions();
            ts.assertComplete();
            ts.assertValue(2);
        } finally {
            RxJavaPlugins.setErrorHandler(handler);
        }
    }

    @Test
    public void testSubscribeWhenPoolClosedEmitsError() throws Exception {
        TestScheduler s = new TestScheduler();
        AtomicInteger count = new AtomicInteger();
        AtomicInteger disposed = new AtomicInteger();
        Pool<Integer> pool = NonBlockingPool //
                .factory(() -> count.incrementAndGet()) //
                .healthCheck(n -> true) //
                .maxSize(3) //
                .maxIdleTime(1, TimeUnit.MINUTES) //
                .disposer(n -> disposed.incrementAndGet()) //
                .scheduler(s) //
                .build();
        pool.close();
        new FlowableSingleDeferUntilRequest<>( //
                pool.member()) //
                        .test(1) //
                        .assertError(PoolClosedException.class) //
                        .assertNoValues();
    }

    @Test
    public void testSubscribeWithDisposedSubscription() throws Exception {
        TestScheduler s = new TestScheduler();
        AtomicInteger count = new AtomicInteger();
        AtomicInteger disposed = new AtomicInteger();
        Pool<Integer> pool = NonBlockingPool //
                .factory(() -> count.incrementAndGet()) //
                .healthCheck(n -> true) //
                .maxSize(3) //
                .maxIdleTime(1, TimeUnit.MINUTES) //
                .disposer(n -> disposed.incrementAndGet()) //
                .scheduler(s) //
                .build();
        AtomicInteger result = new AtomicInteger(0);
        pool.member().subscribe(new SingleObserver<Member<Integer>>() {

            @Override
            public void onSubscribe(Disposable d) {
                d.dispose();
            }

            @Override
            public void onSuccess(Member<Integer> t) {
                result.set(1);
            }

            @Override
            public void onError(Throwable e) {
                e.printStackTrace();
                result.set(2);
            }
        });
        assertEquals(0, result.get());
    }

    @Test
    public void testReentrancyInDrainLoop() throws InterruptedException {
        AtomicInteger count = new AtomicInteger();
        Pool<Integer> pool = NonBlockingPool //
                .factory(() -> count.incrementAndGet()) //
                .healthCheck(n -> true) //
                .maxSize(3) //
                .maxIdleTime(1, TimeUnit.MINUTES) //
                .build();
        AtomicInteger errors = new AtomicInteger();
        CountDownLatch latch = new CountDownLatch(1);
        pool.member() //
                .subscribe(new SingleObserver<Member<Integer>>() {

                    @Override
                    public void onSubscribe(Disposable d) {
                        // ignore
                    }

                    @Override
                    public void onSuccess(Member<Integer> m) {
                        // is emitted by drain loop because scheduler is synchronous
                        pool //
                                .member() //
                                .subscribe(member -> latch.countDown());
                    }

                    @Override
                    public void onError(Throwable e) {
                        errors.incrementAndGet();
                    }
                });
        assertTrue(latch.await(5, TimeUnit.SECONDS));
        assertEquals(0, errors.get());
    }

    @Test
    public void testConcurrentUseWithPoolSizeOf1DoesNotHang() {
        checkDoesNotHang(1);
    }
    
    @Test
    public void testConcurrentUseWithPoolSizeOf2DoesNotHang() {
        checkDoesNotHang(2);
    }
    
    @Test
    public void testConcurrentUseWithPoolSizeOf10DoesNotHang() {
        checkDoesNotHang(10);
    }
    
    private static void checkDoesNotHang(int poolSize) {
        Scheduler io = Schedulers.from(Executors.newFixedThreadPool(2));
        AtomicInteger count = new AtomicInteger();
        Pool<Integer> pool = NonBlockingPool //
                .factory(() -> count.incrementAndGet()) //
                .healthCheck(n -> true) //
                .maxSize(poolSize) //
                .maxIdleTime(1, TimeUnit.MINUTES) //
                .scheduler(io) //
                .build();
        Scheduler scheduler = Schedulers.from(Executors.newFixedThreadPool(poolSize));
        AtomicInteger checkouts = new AtomicInteger();
        Throwable result = Flowable.rangeLong(0, 10000L) //
                .flatMapCompletable((Long n) -> pool.member() //
                        .subscribeOn(scheduler) //
                        .doOnSuccess((Member<Integer> m) -> {
                            checkouts.incrementAndGet();
                            m.checkin();
                        }).ignoreElement()) //
                .blockingGet(60, TimeUnit.SECONDS);
        assertNull(result);
    }

    private static Scheduler createScheduleToDelayCreation(TestScheduler ts) {
        return new Scheduler() {

            @Override
            public Worker createWorker() {
                Worker w = ts.createWorker();
                return new Worker() {

                    @Override
                    public void dispose() {
                        w.dispose();
                    }

                    @Override
                    public boolean isDisposed() {
                        return w.isDisposed();
                    }

                    @Override
                    public Disposable schedule(Runnable run, long delay, TimeUnit unit) {
                        if (run instanceof MemberSingle.Initializer && delay == 0) {
                            return w.schedule(run, 1, TimeUnit.MINUTES);
                        } else {
                            return w.schedule(run, delay, unit);
                        }
                    }
                };
            }

        };
    }

}
