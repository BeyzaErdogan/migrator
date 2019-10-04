package com.inscada.migrator;

public class ConnectionInfo {

    private final String host;
    private final Integer port;
    private final String dbname;
    private final String username;
    private final Integer password;

    public ConnectionInfo(String host, int port, String dbname, String username, int password) {
        this.host = host;
        this.port = port;
        this.dbname = dbname;
        this.username = username;
        this.password = password;
    }

    /**
     * @return the host
     */
    public String getHost() {
        return host;
    }

    /**
     * @return the port
     */
    public Integer getPort() {
        return port;
    }

    /**
     * @return the dbname
     */
    public String getDbname() {
        return dbname;
    }

    /**
     * @return the username
     */
    public String getUsername() {
        return username;
    }

    /**
     * @return the password
     */
    public Integer getPassword() {
        return password;
    }
}
