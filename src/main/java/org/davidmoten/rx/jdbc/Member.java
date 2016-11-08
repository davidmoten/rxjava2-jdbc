package org.davidmoten.rx.jdbc;

import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import io.reactivex.Maybe;
import io.reactivex.MaybeSource;
import io.reactivex.Scheduler.Worker;
import io.reactivex.functions.Consumer;
import io.reactivex.functions.Predicate;
import io.reactivex.schedulers.Schedulers;
import io.reactivex.subjects.PublishSubject;

public final class Member<T> {

    private static final int NOT_INITIALIZED_NOT_IN_USE = 0;
    private static final int INITIALIZED_IN_USE = 1;
    private static final int INITIALIZED_NOT_IN_USE = 2;
    private final AtomicReference<State> state = new AtomicReference<>(new State(NOT_INITIALIZED_NOT_IN_USE));

    private volatile T value;
    private final PublishSubject<Member<T>> subject;
    private final Callable<T> factory;
    private final long retryDelayMs;
    private final Predicate<T> healthy;
    private final Consumer<T> disposer;
    private final Worker worker;

    public Member(PublishSubject<Member<T>> subject, Callable<T> factory, Predicate<T> healthy, Consumer<T> disposer,
            long retryDelayMs) {
        this.subject = subject;
        this.factory = factory;
        this.healthy = healthy;
        this.disposer = disposer;
        this.retryDelayMs = retryDelayMs;
        this.worker = Schedulers.computation().createWorker();
    }

    public Maybe<Member<T>> checkout() {
        return Maybe.defer(() -> {
            // CAS loop for modifications to state of this member
            while (true) {
                State s = state.get();
                if (s.value == NOT_INITIALIZED_NOT_IN_USE) {
                    if (state.compareAndSet(s, new State(INITIALIZED_IN_USE))) {
                        try {
                            value = factory.call();
                        } catch (Throwable e) {
                            return dispose();
                        }
                        return Maybe.just(Member.this);
                    }
                }
                else if (s.value == INITIALIZED_NOT_IN_USE) {
                    if (state.compareAndSet(s, new State(INITIALIZED_IN_USE))) {
                        try {
                            if (healthy.test(value)) {
                                return Maybe.just(Member.this);
                            } else {
                                return dispose();
                            }
                        } catch (Throwable e) {
                            return dispose();
                        }
                    }
                }
                else if (s.value == INITIALIZED_IN_USE) {
                    if (state.compareAndSet(s, new State(INITIALIZED_IN_USE))) {
                        return Maybe.empty();
                    }
                }
            }
        });
    }

    private MaybeSource<? extends Member<T>> dispose() {
        try {
            disposer.accept(value);
        } catch (Throwable t) {
            // ignore
        }
        value = null;
        state.set(new State(NOT_INITIALIZED_NOT_IN_USE));
        //schedule reconsideration of this member in retryDelayMs
        worker.schedule(() -> subject.onNext(Member.this), retryDelayMs, TimeUnit.MILLISECONDS);
        return Maybe.empty();
    }

    public void checkin() {
        state.set(new State(INITIALIZED_NOT_IN_USE));
        subject.onNext(this);
    }

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