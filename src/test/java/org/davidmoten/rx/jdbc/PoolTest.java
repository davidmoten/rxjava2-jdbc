package org.davidmoten.rx.jdbc;

import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Test;

public class PoolTest {
    
    @Test
    public void test() throws InterruptedException {
        AtomicInteger count = new AtomicInteger();
        Pool<Integer> pool = new Pool<Integer>(() -> count.incrementAndGet(), 3);
        pool.members().forEach(System.out::println);
        Thread.sleep(5000);
    }

}
