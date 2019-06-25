package org.davidmoten.rx.pool;

import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.reactivex.disposables.Disposable;
import io.reactivex.plugins.RxJavaPlugins;

final class DecoratingMember<T> implements Member<T> {

    private static final Logger log = LoggerFactory.getLogger(DecoratingMember.class);

    private volatile T value;
    private final MemberSingle<T> memberSingle;
    private final BiFunction<? super T, ? super Checkin, ? extends T> checkinDecorator;

    // synchronized by MemberSingle.drain() wip
    private Disposable scheduled;

    // synchronized by MemberSingle.drain() wip
    private boolean releasing;

    // synchronized by MemberSingle.drain() wip
    private boolean checking;

    // synchronized by MemberSingle.drain() wip
    // not subject to word tearing, because of ordering in drain loop (will only
    // read this value if check has finished)
    private long lastCheckTime;

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

    public void markAsChecking() {
        this.checking = true;
    }

    public boolean isChecking() {
        return checking;
    }

    @Override
    public void disposeValue() {
        try {
            if (scheduled != null) {
                scheduled.dispose();
                scheduled = null;
            }
            log.debug("disposing value {}", value);
            memberSingle.pool.disposer.accept(value);
        } catch (Throwable e) {
            // make action configurable
            RxJavaPlugins.onError(e);
        } finally {
            value = null;
            checking = false;
        }
    }

    public void setValueAndClearReleasingFlag(T value) {
        this.value = value;
        this.releasing = false;
        this.lastCheckTime = now();
    }

    void scheduleRelease() {
        if (scheduled != null) {
            scheduled.dispose();
            log.debug("cancelled scheduled release of {}", this);
        }
        long maxIdleTimeMs = memberSingle.pool.maxIdleTimeMs;
        if (maxIdleTimeMs > 0) {
            // TODO make `this` runnable to save lambda allocation
            scheduled = memberSingle.pool.scheduler //
                    .scheduleDirect( //
                            () -> memberSingle.addToBeReleased(this), //
                            maxIdleTimeMs, //
                            TimeUnit.MILLISECONDS);
            log.debug("scheduled release in {}ms of {}", maxIdleTimeMs, this);
        }
    }

    @Override
    public String toString() {
        return "DecoratingMember [value=" + value + "]";
    }

    public void markAsChecked() {
        checking = false;
        lastCheckTime = now();
    }

    private long now() {
        return memberSingle.pool.scheduler.now(TimeUnit.MILLISECONDS);
    }

    public long lastCheckTime() {
        return lastCheckTime;
    }

}
