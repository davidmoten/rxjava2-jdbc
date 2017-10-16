package org.davidmoten.rx.jdbc.pool;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import io.reactivex.Scheduler;
import io.reactivex.schedulers.Schedulers;

public final class ExecutorScheduler extends Scheduler {

    private final ExecutorService executor;
    private final Scheduler scheduler;

    public ExecutorScheduler(ExecutorService executor) {
        this.executor = executor;
        this.scheduler = Schedulers.from(executor);
    }

    @Override
    public Worker createWorker() {
        return scheduler.createWorker();
    }

    @Override
    public void shutdown() {
        scheduler.shutdown();
        executor.shutdown();
        try {
            executor.awaitTermination(60, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

}
