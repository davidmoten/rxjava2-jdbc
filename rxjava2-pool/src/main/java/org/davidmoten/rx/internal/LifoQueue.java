package org.davidmoten.rx.internal;

import java.util.concurrent.atomic.AtomicReference;

import io.reactivex.annotations.NonNull;
import io.reactivex.annotations.Nullable;

public final class LifoQueue<T> {

    private final AtomicReference<Node<T>> head = new AtomicReference<>();

    public void offer(@NonNull T t) {
        while (true) {
            Node<T> a = head.get();
            Node<T> b = new Node<>(t, a);
            if (head.compareAndSet(a, b)) {
                return;
            }
        }
    }

    public T poll() {
        Node<T> a = head.get();
        if (a == null) {
            return null;
        } else {
            while (true) {
                if (head.compareAndSet(a, a.next)) {
                    return a.value;
                } else {
                    a = head.get();
                }
            }
        }
    }
    
    public void clear() {
        head.set(null);
    }

    static final class Node<T> {
        final @NonNull T value;
        final @Nullable Node<T> next;

        Node(T value, Node<T> next) {
            this.value = value;
            this.next = next;
        }
    }

}
