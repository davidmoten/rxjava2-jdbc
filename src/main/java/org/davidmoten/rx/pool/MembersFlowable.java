package org.davidmoten.rx.pool;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import io.reactivex.Flowable;
import io.reactivex.Scheduler;
import io.reactivex.Scheduler.Worker;
import io.reactivex.internal.fuseable.SimplePlainQueue;
import io.reactivex.internal.queue.MpscLinkedQueue;

class MembersFlowable<T> extends Flowable<Member<T>> implements Subscription, Closeable {

    private final SimplePlainQueue<Member<T>> queue;
    private final AtomicLong requested = new AtomicLong();
    private final AtomicInteger wip = new AtomicInteger();
    private final Member<T>[] members;
    private final Scheduler scheduler;
    private final MemberFactory<T, NonBlockingPool2<T>> memberFactory;
    private final int maxSize;

    // mutable

    // only set once
    private Subscriber<? super Member<T>> child;

    private volatile boolean cancelled;

    // number of members in the pool at the moment
    private int count;

    MembersFlowable(NonBlockingPool2<T> pool) {
        this.queue = new MpscLinkedQueue<Member<T>>();
        this.members = createMembersArray(pool);
        this.count = 0;
        this.scheduler = pool.scheduler;
        this.maxSize = pool.maxSize;
        this.memberFactory = pool.memberFactory;
    }

    private static <T> Member<T>[] createMembersArray(NonBlockingPool2<T> pool) {
        @SuppressWarnings("unchecked")
        Member<T>[] m = new Member[pool.maxSize];
        for (int i = 0; i < m.length; i++) {
            m[i] = pool.memberFactory.create(pool);
        }
        return m;
    }

    @Override
    protected void subscribeActual(Subscriber<? super Member<T>> child) {
        // expect only one subscriber
        // use .publish() on this if more than one required
        this.child = child;
    }

    public void checkin(Member<T> member) {
        queue.offer(member);
        drain();
    }

    @Override
    public void request(long n) {
        drain();
    }

    @Override
    public void cancel() {
        this.cancelled = true;
    }

    private void drain() {
        if (wip.getAndIncrement() == 0) {
            int missed = 0;
            while (true) {
                long r = requested.get();
                long e = 0;
                while (e != r) {
                    if (cancelled) {
                        queue.clear();
                        return;
                    }
                    Member<T> m = queue.poll();
                    if (m == null) {
                        if (count < maxSize) {
                            // haven't used all the members of the pool yet
                            emit(members[count]);
                            count++;
                            e++;
                        } else {
                            // nothing to emit and not done
                            break;
                        }
                    } else {
                        emit(m);
                        e++;
                    }
                }
                if (e != 0 && r != Long.MAX_VALUE) {
                    requested.addAndGet(-e);
                }
                missed = wip.addAndGet(-missed);
                if (missed == 0) {
                    return;
                }
            }
        }
    }

    private void emit(Member<T> m) {
        // get a fresh worker each time so we jump threads to
        // break the stack-trace (a long-enough chain of
        // checkout-checkins could otherwise provoke stack
        // overflow)
        Worker worker = scheduler.createWorker();
        worker.schedule(new Emitter<T>(worker, child, m));
    }

    @Override
    public void close() throws IOException {
        for (Member<T> member : members) {
            try {
                member.close();
            } catch (Exception e) {
                // TODO accumulate and throw?
                e.printStackTrace();
            }
        }
    }

    private static final class Emitter<T> implements Runnable {

        private final Worker worker;
        private final Subscriber<? super Member<T>> child;
        private final Member<T> m;

        Emitter(Worker worker, Subscriber<? super Member<T>> child, Member<T> m) {
            this.worker = worker;
            this.child = child;
            this.m = m;

        }

        @Override
        public void run() {
            child.onNext(m);
            worker.dispose();
        }
    }

}
