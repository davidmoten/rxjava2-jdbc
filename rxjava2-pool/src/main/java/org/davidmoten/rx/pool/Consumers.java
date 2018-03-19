package org.davidmoten.rx.pool;

import io.reactivex.functions.Consumer;

final class Consumers {

    private Consumers() {
        // prevent instantiation
    }

    static final class DoNothingHolder {
        static final Consumer<Object> value = new Consumer<Object>() {

            @Override
            public void accept(Object arg0) throws Exception {
                // do nothing
            }

        };
    }

    @SuppressWarnings("unchecked")
    static <T> Consumer<T> doNothing() {
        return (Consumer<T>) DoNothingHolder.value;
    }

}
