package org.davidmoten.rx.pool;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.reactivex.Maybe;
import io.reactivex.Scheduler.Worker;
import io.reactivex.disposables.Disposable;
import io.reactivex.internal.disposables.DisposableHelper;
import io.reactivex.plugins.RxJavaPlugins;

public final class NonBlockingMember<T> implements Member<T> {

    private static final Logger log = LoggerFactory.getLogger(NonBlockingMember.class);

    private static final int NOT_INITIALIZED_NOT_IN_USE = 0;
    private static final int INITIALIZED_IN_USE = 1;
    private static final int INITIALIZED_NOT_IN_USE = 2;
    private static final int DISPOSING = 3;

    /**
     * <pre>
     * checkout:  NOT_INITIALIZED_NOT_IN_USE -> INITIALIZED_IN_USE 
     * checkin:   INITIALIZED_IN_USE         -> INITIALIZED_NOT_IN_USE
     * dispose:   INITIALIZED_IN_USE         -> DISPOSING 
     *                                       -> NOT_INITIALIZED_NOT_IN_USE
     * </pre>
     */
    private final AtomicReference<State> state = new AtomicReference<>(
            new State(NOT_INITIALIZED_NOT_IN_USE, DisposableHelper.DISPOSED, true));
    private final Worker worker;
    private final NonBlockingPool<T> pool;
    private final Member<T> proxy;

    // mutable
    private volatile T value;
    private volatile long lastCheckoutTime;

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
                if (s.state == NOT_INITIALIZED_NOT_IN_USE) {
                    log.debug("checking out member not initialized={}", this);
                    if (s.enabled) {
                        if (state.compareAndSet(s, new State(INITIALIZED_IN_USE, s.idleTimeoutClose, s.enabled))) {
                            try {
                                // this action might block (it does in the JDBC
                                // 4 Connection case)
                                value = pool.factory.call();
                            } catch (Throwable e) {
                                RxJavaPlugins.onError(e);
                                return disposeAndReturnToPool();
                            }
                            // we don't do a health check on a just-created
                            // connection
                            lastCheckoutTime = pool.scheduler.now(TimeUnit.MILLISECONDS);
                            log.debug("initialized in use: member={}", this);
                            return Maybe.just(ifNull(proxy, NonBlockingMember.this));
                        }
                    } else {
                        if (state.compareAndSet(s, s.copy())) {
                            return Maybe.empty();
                        }
                    }
                } else if (s.state == INITIALIZED_NOT_IN_USE) {
                    log.debug("checking out member not in use={}", this);
                    if (state.compareAndSet(s, new State(INITIALIZED_IN_USE, DisposableHelper.DISPOSED, s.enabled))) {
                        // cancel the idle timeout
                        s.idleTimeoutClose.dispose();
                        long now = pool.scheduler.now(TimeUnit.MILLISECONDS);
                        long last = lastCheckoutTime;
                        boolean checkOk = now < last + pool.idleTimeBeforeHealthCheckMs;
                        if (!checkOk) {
                            try {
                                checkOk = pool.healthy.test(value);
                            } catch (Throwable e) {
                                checkOk = false;
                            }
                        }
                        if (checkOk) {
                            log.debug("initialized in use: member={}", this);
                            lastCheckoutTime = now;
                            return Maybe.just(ifNull(proxy, NonBlockingMember.this));
                        } else {
                            log.debug("initialized not healthy: member={}", this);
                            return disposeAndReturnToPool();
                        }

                    }
                } else if (s.state == INITIALIZED_IN_USE || s.state == DISPOSING) {
                    if (state.compareAndSet(s, s.copy())) {
                        return Maybe.empty();
                    }
                }
            }
        });
    }

    @Override
    public void checkin() {
        log.debug("checking in member {}", this);
        while (true) {
            State s = state.get();
            if (s.state == INITIALIZED_IN_USE) {
                if (s.enabled) {
                    Resetter<T> resetter = new Resetter<>(this);
                    Disposable sub = worker.schedule(resetter, //
                            pool.maxIdleTimeMs, TimeUnit.MILLISECONDS);
                    if (state.compareAndSet(s, new State(INITIALIZED_NOT_IN_USE, sub, s.enabled))) {
                        resetter.enable();
                        pool.subject.onNext(this);
                        break;
                    } else {
                        sub.dispose();
                    }
                } else {
                    // not enabled (shutting down)
                    if (state.compareAndSet(s, new State(INITIALIZED_NOT_IN_USE, null, s.enabled))) {
                        disposePermanently();
                    }
                    break;
                }
            } else if (state.compareAndSet(s, s.copy())) {
                break;
            }
        }
    }

    private Maybe<Member<T>> disposeAndReturnToPool() {
        return dispose(true);
    }

    private Maybe<Member<T>> disposePermanently() {
        return dispose(false);
    }

    private Maybe<Member<T>> dispose(boolean returnToPool) {

        while (true) {
            State s = state.get();
            if (s.state == INITIALIZED_IN_USE
                    && state.compareAndSet(s, new State(DISPOSING, DisposableHelper.DISPOSED, s.enabled))) {
                T v = value;
                value = null;
                if (v != null) {
                    try {
                        pool.disposer.accept(v);
                    } catch (Throwable t) {
                        RxJavaPlugins.onError(t);
                    }
                }
                s.idleTimeoutClose.dispose();
                state.set(new State(NOT_INITIALIZED_NOT_IN_USE, DisposableHelper.DISPOSED, s.enabled));
                if (returnToPool) {
                    // schedule reconsideration of this member in retryDelayMs
                    worker.schedule(() -> pool.subject.onNext(NonBlockingMember.this), //
                            pool.returnToPoolDelayAfterHealthCheckFailureMs, TimeUnit.MILLISECONDS);
                }
                break;
            } else if (state.compareAndSet(s, s.copy())) {
                break;
            }
        }
        return Maybe.empty();
    }

    private static final class Resetter<T> implements Runnable {

        private final NonBlockingMember<T> m;
        private volatile boolean enabled = false;

        Resetter(NonBlockingMember<T> m) {
            this.m = m;
        }

        @Override
        public void run() {
            if (enabled) {
                m.reset();
            }
        }

        void enable() {
            enabled = true;
        }

    }

    private void reset() {
        // called after idle timeout expires
        log.debug("resetting member {}", this);
        while (true) {
            State s = state.get();
            if (s.state == INITIALIZED_NOT_IN_USE) {
                if (state.compareAndSet(s, new State(NOT_INITIALIZED_NOT_IN_USE, s.idleTimeoutClose, s.enabled))) {
                    pool.subject.onNext(this);
                    break;
                }
            } else if (state.compareAndSet(s, s.copy())) {
                break;
            }
        }
    }

    @Override
    public T value() {
        return value;
    }

    private static final class State {
        final int state;
        final Disposable idleTimeoutClose;
        final boolean enabled;

        State(int name, Disposable idleTimeoutClose, boolean enabled) {
            this.state = name;
            this.idleTimeoutClose = idleTimeoutClose;
            this.enabled = enabled;
        }

        State copy() {
            return new State(state, idleTimeoutClose, enabled);
        }
    }

    @Override
    public void shutdown() {
        while (true) {
            State s = state.get();
            if (state.compareAndSet(s, new State(s.state, s.idleTimeoutClose, false))) {
                worker.dispose();
                return;
            }
        }
    }

    private static <T> Member<T> ifNull(Member<T> proxy, Member<T> other) {
        if (proxy == null) {
            return other;
        } else {
            return proxy;
        }
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

    @Override
    public void close() throws Exception {
        // TODO is close needed (not covered)?
        shutdown();
    }

}