package org.davidmoten.rx.pool;

import java.util.concurrent.TimeUnit;

import io.reactivex.Scheduler;

public final class Clock {

    private final Scheduler scheduler;

    public Clock(Scheduler scheduler) {
        this.scheduler = scheduler;
    }

    public long now() {
        return scheduler.now(TimeUnit.MILLISECONDS);
    }
}
