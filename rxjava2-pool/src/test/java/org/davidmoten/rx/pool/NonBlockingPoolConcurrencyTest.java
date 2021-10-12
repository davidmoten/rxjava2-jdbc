package org.davidmoten.rx.pool;

import static org.junit.Assert.assertEquals;

import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.Test;

import io.reactivex.Flowable;
import io.reactivex.schedulers.Schedulers;

public class NonBlockingPoolConcurrencyTest {

    @Test
    public void memberSingleCoverage() throws Exception {
        // attempt to get coverage of x.activeCount > 0 expression in tryEmit
        // has not been successful but covers the other statements in tryEmit
        AtomicLong count = new AtomicLong();
        AtomicLong disposed = new AtomicLong();
        int poolSize = 4;
        try (Pool<Long> pool = NonBlockingPool //
                .factory(() -> count.incrementAndGet()) //
                .healthCheck(n -> true) //
                .maxSize(poolSize) //
                .maxIdleTime(1, TimeUnit.MINUTES) //
                .disposer(n -> disposed.incrementAndGet()) //
                .build()) {
            long n = Long.parseLong(System.getProperty("n", "100000"));
            long[] c = new long[1];
            Flowable //
                    .rangeLong(0, n) //
                    .flatMapSingle(x -> pool.member(), false, poolSize) //
                    .doOnNext(x -> c[0]++) //
                    // have to keep the observeOn buffer small so members don't get buffered
                    // and not checked in
                    .observeOn(Schedulers.from(Executors.newFixedThreadPool(1)), false, 1) //
                    .doOnNext(member -> member.checkin()) //
                    .timeout(10, TimeUnit.SECONDS) //
                    .doOnError(e -> {
                        System.out.println("emitted " + c[0] + ", count=" + count);
                    }) //
                    .count() //
                    .blockingGet();
            // note that the last member in particular may
            assertEquals(c[0], n);
            assertEquals(0, disposed.get());
        }
        assertEquals(poolSize, disposed.get());
    }

}
