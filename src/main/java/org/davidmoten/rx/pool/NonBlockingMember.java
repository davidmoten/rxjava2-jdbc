package org.davidmoten.rx.pool;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.reactivex.Maybe;
import io.reactivex.MaybeSource;
import io.reactivex.Scheduler.Worker;

public class NonBlockingMember<T> implements Member<T> {

    private static final Logger log = LoggerFactory.getLogger(NonBlockingMember.class);

    private static final int NOT_INITIALIZED_NOT_IN_USE = 0;
    private static final int INITIALIZED_IN_USE = 1;
    private static final int INITIALIZED_NOT_IN_USE = 2;

    private final AtomicReference<State> state = new AtomicReference<>(
            new State(NOT_INITIALIZED_NOT_IN_USE));

    private volatile T value;

    private final Worker worker;
    private final NonBlockingPool<T> pool;
    private final Member<T> proxy;

    public NonBlockingMember(NonBlockingPool<T> pool, Member<T> proxy) {
        this.pool = pool;
        this.proxy = proxy;
        this.worker = pool.scheduler.createWorker();
    }

    @Override
    public Maybe<Member<T>> checkout() {
        return Maybe.defer(() -> {
            // CAS loop for modifications to state of this member
            while (true) {
                State s = state.get();

                if (s.value == NOT_INITIALIZED_NOT_IN_USE) {
                    log.debug("checking out member not initialized={}", this);
                    if (state.compareAndSet(s, new State(INITIALIZED_IN_USE))) {
                        try {
                            value = pool.factory.call();
                        } catch (Throwable e) {
                            return dispose();
                        }
                        log.debug("initialized in use: member={}", this);
                        return Maybe.just(ifNull(proxy, NonBlockingMember.this));
                    }
                } else if (s.value == INITIALIZED_NOT_IN_USE) {
                    log.debug("checking out member not in use={}", this);
                    if (state.compareAndSet(s, new State(INITIALIZED_IN_USE))) {
                        try {
                            if (pool.healthy.test(value)) {
                                log.debug("initialized in use: member={}", this);
                                return Maybe.just(ifNull(proxy, NonBlockingMember.this));
                            } else {
                                log.debug("initialized not healthy: member={}", this);
                                return dispose();
                            }
                        } catch (Throwable e) {
                            return dispose();
                        }
                    }
                } else if (s.value == INITIALIZED_IN_USE) {
                    log.debug("checking out member={}", this);
                    if (state.compareAndSet(s, new State(INITIALIZED_IN_USE))) {
                        log.debug("already in use, member={}", this);
                        return Maybe.empty();
                    }
                }
            }
        });
    }

    private Member<T> ifNull(Member<T> proxy, Member<T> other) {
        if (proxy == null)
            return other;
        else
            return proxy;
    }

    private MaybeSource<? extends Member<T>> dispose() {
        try {
            pool.disposer.accept(value);
        } catch (Throwable t) {
            // ignore
        }
        value = null;
        state.set(new State(NOT_INITIALIZED_NOT_IN_USE));
        // schedule reconsideration of this member in retryDelayMs
        worker.schedule(() -> pool.subject.onNext(NonBlockingMember.this), pool.retryDelayMs,
                TimeUnit.MILLISECONDS);
        return Maybe.empty();
    }

    @Override
    public void checkin() {
        state.set(new State(INITIALIZED_NOT_IN_USE));
        pool.subject.onNext(this);
    }

    @Override
    public T value() {
        return value;
    }

    private static final class State {
        final int value;

        State(int value) {
            this.value = value;
        }
    }

    @Override
    public void close() throws Exception {
        worker.dispose();
        // TODO
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("Member [value=");
        builder.append(value);
        builder.append(", state=");
        builder.append(state.get());
        builder.append("]");
        return builder.toString();
    }

}