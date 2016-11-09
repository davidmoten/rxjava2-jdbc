package org.davidmoten.rx.jdbc.pool;

import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Test;

import io.reactivex.schedulers.Schedulers;

public class PoolTest {

    @Test
    public void test() throws InterruptedException {
        AtomicInteger count = new AtomicInteger();
        MemberFactory<Integer, NonBlockingPool<Integer>> memberFactory = pool -> new NonBlockingMember<Integer>(
                pool);
        Pool<Integer> pool = new NonBlockingPool<Integer>( //
                () -> count.incrementAndGet(), //
                n -> true, //
                n -> {
                } , //
                3, //
                1000, //
                memberFactory, //
                Schedulers.computation());
        pool.members() //
                .forEach(System.out::println);
    }

}
