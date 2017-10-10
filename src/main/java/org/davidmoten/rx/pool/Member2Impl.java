package org.davidmoten.rx.pool;

import java.util.concurrent.TimeUnit;

import io.reactivex.disposables.Disposable;
import io.reactivex.plugins.RxJavaPlugins;

public final class Member2Impl<T> implements Member2<T> {

    private volatile T value;
    private final MemberSingle2<T> memberSingle;

    // synchronized by MemberSingle.drain() wip
    private Disposable scheduled;

    Member2Impl(T value, MemberSingle2<T> memberSingle) {
        this.memberSingle = memberSingle;
        this.value = value;
    }

    @Override
    public void checkin() {
        memberSingle.pool.checkin(this);
    }

    @Override
    public void disposeValue() {
        try {
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

    @Override
    public T value() {
        return value;
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
