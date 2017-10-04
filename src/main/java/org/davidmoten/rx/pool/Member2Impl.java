package org.davidmoten.rx.pool;

import java.util.concurrent.atomic.AtomicInteger;

import io.reactivex.disposables.CompositeDisposable;
import io.reactivex.internal.fuseable.SimplePlainQueue;
import io.reactivex.internal.queue.MpscLinkedQueue;
import io.reactivex.plugins.RxJavaPlugins;

public class Member2Impl<T> extends AtomicInteger implements Member2<T> {

    private static final long serialVersionUID = 1283781357247917404L;

    private T value;
    private final MemberSingle2<T> memberSingle;
    private final CompositeDisposable disposables = new CompositeDisposable();
    private final SimplePlainQueue<Integer> queue = new MpscLinkedQueue<Integer>();
    private static final Integer ACTION_CHECKIN = 1;
    private static final Integer ACTION_CHECKOUT = 2;
    private static final Integer ACTION_RELEASE = 3;

    Member2Impl(T value, MemberSingle2<T> memberSingle) {
        this.value = value;
        this.memberSingle = memberSingle;
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

    private void drain() {
        if (this.getAndIncrement()==0) {
            int missed = 0;
            while (true) {
                while (true) {
                    Integer action = queue.poll();
                    if (action==null) {
                        break;
                    } else {
                        if (action == ACTION_CHECKIN) {
                            memberSingle.pool.checkin(this);
                        }
                    }
                }
            }
        }
    }

}
