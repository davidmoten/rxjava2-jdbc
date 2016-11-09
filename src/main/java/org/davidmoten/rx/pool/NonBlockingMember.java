package org.davidmoten.rx.pool;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import io.reactivex.Maybe;
import io.reactivex.MaybeSource;
import io.reactivex.Scheduler.Worker;
import io.reactivex.subjects.PublishSubject;
import io.reactivex.subjects.Subject;

public final class NonBlockingMember<T> implements Member<T> {

    private static final int NOT_INITIALIZED_NOT_IN_USE = 0;
    private static final int INITIALIZED_IN_USE = 1;
    private static final int INITIALIZED_NOT_IN_USE = 2;
    private final AtomicReference<State> state = new AtomicReference<>(
            new State(NOT_INITIALIZED_NOT_IN_USE));

    private volatile T value;
    private final Subject<Member<T>> subject;

    private final Worker worker;
    private final NonBlockingPool<T> pool;

    public NonBlockingMember(NonBlockingPool<T> pool) {
        this.pool = pool;
        this.worker = pool.scheduler.createWorker();
        this.subject = PublishSubject.<Member<T>> create().toSerialized();
    }

    @Override
    public Maybe<Member<T>> checkout() {
        return Maybe.defer(() -> {
            // CAS loop for modifications to state of this member
            while (true) {
                State s = state.get();
                if (s.value == NOT_INITIALIZED_NOT_IN_USE) {
                    if (state.compareAndSet(s, new State(INITIALIZED_IN_USE))) {
                        try {
                            value = pool.factory.call();
                        } catch (Throwable e) {
                            return dispose();
                        }
                        return Maybe.just(NonBlockingMember.this);
                    }
                } else if (s.value == INITIALIZED_NOT_IN_USE) {
                    if (state.compareAndSet(s, new State(INITIALIZED_IN_USE))) {
                        try {
                            if (pool.healthy.test(value)) {
                                return Maybe.just(NonBlockingMember.this);
                            } else {
                                return dispose();
                            }
                        } catch (Throwable e) {
                            return dispose();
                        }
                    }
                } else if (s.value == INITIALIZED_IN_USE) {
                    if (state.compareAndSet(s, new State(INITIALIZED_IN_USE))) {
                        return Maybe.empty();
                    }
                }
            }
        });
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
        worker.schedule(() -> subject.onNext(NonBlockingMember.this), pool.retryDelayMs,
                TimeUnit.MILLISECONDS);
        return Maybe.empty();
    }

    @Override
    public void checkin() {
        state.set(new State(INITIALIZED_NOT_IN_USE));
        subject.onNext(this);
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