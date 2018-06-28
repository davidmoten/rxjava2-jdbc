package org.davidmoten.rx.jdbc.pool;

public interface ConnectionListener {
    
    void onSuccess();
    
    void onError(Throwable error);

}
