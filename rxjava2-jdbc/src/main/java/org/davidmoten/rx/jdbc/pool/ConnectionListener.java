package org.davidmoten.rx.jdbc.pool;

/**
 * Receives the results of connection attempts. Events are reported to the
 * listener serially (i.e. the methods {@code onSuccess} and {@code onError}
 * will not be called concurrently.
 *
 */
public interface ConnectionListener {

    void onSuccess();

    void onError(Throwable error);

}
