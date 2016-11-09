package org.davidmoten.rx.jdbc.pool;

import java.sql.Connection;
import java.util.concurrent.atomic.AtomicInteger;

import org.davidmoten.rx.jdbc.Database;
import org.davidmoten.rx.pool.MemberFactory;
import org.davidmoten.rx.pool.NonBlockingMember;
import org.davidmoten.rx.pool.NonBlockingPool;
import org.davidmoten.rx.pool.Pool;
import org.junit.Test;

import io.reactivex.BackpressureStrategy;
import io.reactivex.Flowable;
import io.reactivex.schedulers.Schedulers;
import io.reactivex.subjects.PublishSubject;
import io.reactivex.subjects.Subject;
import io.reactivex.subscribers.TestSubscriber;

public class PoolTest {

    @Test
    public void test() throws InterruptedException {
        AtomicInteger count = new AtomicInteger();
        MemberFactory<Integer, NonBlockingPool<Integer>> memberFactory = pool -> new NonBlockingMember<Integer>(
                pool, null);
        Pool<Integer> pool = NonBlockingPool.factory(() -> count.incrementAndGet())
                .healthy(n -> true).disposer(n -> {
                }).maxSize(3).retryDelayMs(1000).memberFactory(memberFactory)
                .scheduler(Schedulers.computation()).build();
        pool.members() //
                .forEach(System.out::println);
    }

    @Test
    public void testConnectionPoolRecyles() {
        Database db = DatabaseCreator.create(2);
        TestSubscriber<Connection> ts = db.connections() //
                .doOnNext(System.out::println) //
                .doOnNext(c -> {
                    c.close();
                }) //
                .test(0); //
        ts.request(10);
        ts.assertValueCount(10) //
                .assertNotTerminated();
    }

    @Test
    public void test2() {
        Subject<Integer> subject = PublishSubject.<Integer> create().toSerialized();
        subject.toFlowable(BackpressureStrategy.BUFFER) //
                .mergeWith(Flowable.range(1, 5)) //
                .doOnNext(n -> subject.onNext(10)) //
                .doOnNext(System.out::println) //
                .take(8) //
                .subscribe();
        subject.onNext(2);
    }

    @Test
    public void test3() {
        Flowable.range(1, 4).mergeWith(Flowable.range(100, 2)).take(10)
                .doOnNext(System.out::println).subscribe();
    }

}
