package org.davidmoten.rx.jdbc;

public final class ConnectionProviders {
    
    public static ConnectionProvider test() {
        return Database.testConnectionProvider();
    }
    
    public static ConnectionProvider from(String url) {
        return Util.connectionProvider(url);
    }

}
