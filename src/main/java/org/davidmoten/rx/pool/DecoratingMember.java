package org.davidmoten.rx.pool;

import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;

import io.reactivex.disposables.Disposable;
import io.reactivex.plugins.RxJavaPlugins;

final class DecoratingMember<T> implements Member<T> {

    private volatile T value;
    private final MemberSingle<T> memberSingle;
    private final BiFunction<? super T, ? super Checkin, ? extends T> checkinDecorator;

    // synchronized by MemberSingle.drain() wip
    private Disposable scheduled;

    // synchronized by MemberSingle.drain() wip
    private boolean releasing;

    DecoratingMember(T value, BiFunction<? super T, ? super Checkin, ? extends T> checkinDecorator,
            MemberSingle<T> memberSingle) {
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

    public void markAsReleasing() {
        this.releasing = true;
    }

    public boolean isReleasing() {
        return releasing;
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

    public void setValueAndClearReleasingFlag(T value) {
        this.value = value;
        this.releasing = false;
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
        return "DecoratingMember [value=" + value + "]";
    }

}
