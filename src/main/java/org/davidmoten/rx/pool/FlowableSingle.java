package org.davidmoten.rx.pool;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import io.reactivex.Flowable;
import io.reactivex.Single;
import io.reactivex.SingleObserver;
import io.reactivex.disposables.Disposable;
import io.reactivex.disposables.Disposables;

public class FlowableSingle<T> extends Flowable<T> {

    private final Single<T> single;

    public FlowableSingle(Single<T> single) {
        this.single = single;
    }

    @Override
    protected void subscribeActual(Subscriber<? super T> s) {
        SingleSubscription<T> sub = new SingleSubscription<T>(single, s);
        s.onSubscribe(sub);
    }

    private static final class SingleSubscription<T> implements Subscription, SingleObserver<T> {

        private final Subscriber<? super T> s;
        private final Single<T> single;
        private final AtomicBoolean once = new AtomicBoolean();
        private final AtomicReference<Disposable> disposable = new AtomicReference<Disposable>();

        public SingleSubscription(Single<T> single, Subscriber<? super T> s) {
            this.single = single;
            this.s = s;
        }

        @Override
        public void request(long n) {
            if (n > 0 && once.compareAndSet(false, true)) {
                Disposable d = disposable.get();
                if (d == null) {
                    single.subscribe(this);
                }
            }
        }

        @Override
        public void cancel() {
            if (disposable.compareAndSet(null, Disposables.disposed())) {
                return;
            } else {
                disposable.get().dispose();
            }
        }

        @Override
        public void onSubscribe(Disposable d) {
            if (!disposable.compareAndSet(null, d)) {
                //already cancelled
                d.dispose();
            }
        }

        @Override
        public void onSuccess(T t) {
            s.onNext(t);
            s.onComplete();
        }

        @Override
        public void onError(Throwable e) {
            s.onError(e);
        }

    }

}
