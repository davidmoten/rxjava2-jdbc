package org.davidmoten.rx.pool;

import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;

import io.reactivex.disposables.Disposable;
import io.reactivex.plugins.RxJavaPlugins;

public final class Member2Impl<T> implements Member2<T> {

    private volatile T value;
    private final MemberSingle2<T> memberSingle;
    private final BiFunction<T, Checkin, T> checkinDecorator;

    // synchronized by MemberSingle.drain() wip
    private Disposable scheduled;

    Member2Impl(T value, BiFunction<T, Checkin, T> checkinDecorator,
            MemberSingle2<T> memberSingle) {
        this.checkinDecorator = checkinDecorator;
        this.memberSingle = memberSingle;
        this.value = value;
    }

    @Override
    public T value() {
        return checkinDecorator.apply(value, this);
    }

    @Override
    public void checkin() {
        memberSingle.pool.checkin(this);
    }

    @Override
    public void disposeValue() {
        try {
            if (scheduled != null) {
                scheduled.dispose();
                scheduled = null;
            }
            memberSingle.pool.disposer.accept(value);
            value = null;
        } catch (Throwable e) {
            // make action configurable
            RxJavaPlugins.onError(e);
            value = null;
        }
    }

    public void release() {
        disposeValue();
        memberSingle.release(this);
    }

    public void setValue(T value) {
        this.value = value;
    }

    void scheduleRelease() {
        if (scheduled != null) {
            scheduled.dispose();
        }
        // TODO make `this` runnable to save lambda allocation
        scheduled = memberSingle.pool.scheduler.scheduleDirect(() -> {
            release();
        }, memberSingle.pool.releaseIntervalMs, TimeUnit.MILLISECONDS);
    }

    void preCheckout() {
        if (scheduled != null) {
            scheduled.dispose();
            scheduled = null;
        }
    }

    @Override
    public String toString() {
        return "Member2Impl [value=" + value + "]";
    }

}
