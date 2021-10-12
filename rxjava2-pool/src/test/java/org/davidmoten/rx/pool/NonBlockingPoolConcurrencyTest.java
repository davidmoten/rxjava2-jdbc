package org.davidmoten.rx.pool;

import static org.junit.Assert.assertEquals;

import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.Test;

import io.reactivex.Flowable;
import io.reactivex.schedulers.Schedulers;

public class NonBlockingPoolConcurrencyTest {

    @Test(timeout=30000)
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
            Flowable //
                    .rangeLong(0, 100000) //
                    .flatMapSingle(x -> pool.member(), false, 4) //
                    .observeOn(Schedulers.from(Executors.newFixedThreadPool(1))) //
                    .doOnNext(member -> member.checkin()) //
                    .count() //
                    .blockingGet();
            assertEquals(poolSize, count.get());
            assertEquals(0, disposed.get());
        }
        assertEquals(poolSize, disposed.get());
    }

}
