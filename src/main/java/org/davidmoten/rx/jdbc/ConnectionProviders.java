package org.davidmoten.rx.jdbc;

public final class ConnectionProviders {
    
    private ConnectionProviders() {
        //prevent instantiation
    }
    
    public static ConnectionProvider test() {
        return Database.testConnectionProvider();
    }
    
    public static ConnectionProvider from(String url) {
        return Util.connectionProvider(url);
    }

}
