package org.davidmoten.rx.jdbc;

import javax.annotation.Nonnull;

import com.github.davidmoten.guavamini.Preconditions;

import io.reactivex.Completable;
import io.reactivex.Flowable;
import io.reactivex.Observable;
import io.reactivex.Single;

public interface DependsOn<T> {

    T dependsOn(Flowable<?> flowable);

    default T dependsOn(@Nonnull Observable<?> observable) {
        Preconditions.checkNotNull(observable, "observable cannot be null");
        return dependsOn(observable.ignoreElements().toFlowable());
    }

    default T dependsOn(@Nonnull Single<?> single) {
        Preconditions.checkNotNull(single, "single cannot be null");
        return dependsOn(single.toFlowable());
    }

    default T dependsOn(@Nonnull Completable completable) {
        Preconditions.checkNotNull(completable, "completable cannot be null");
        return dependsOn(completable.toFlowable());
    }

}
