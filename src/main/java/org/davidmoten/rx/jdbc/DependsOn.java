package org.davidmoten.rx.jdbc;

import io.reactivex.Completable;
import io.reactivex.Flowable;
import io.reactivex.Observable;
import io.reactivex.Single;

public interface DependsOn<T> {

    T dependsOn(Flowable<?> flowable);

    default T dependsOn(Observable<?> observable) {
        return dependsOn(observable.ignoreElements().toFlowable());
    }

    default T dependsOn(Single<?> single) {
        return dependsOn(single.toFlowable());
    }

    default T dependsOn(Completable completable) {
        return dependsOn(completable.toFlowable());
    }

}
