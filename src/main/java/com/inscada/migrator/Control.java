
package com.inscada.migrator;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class Control {
    private final Integer port;
    private final String host;
    private final String dbName;
    private final String username;
    private final Integer password;
    Connection postgresConnection = null;
    private Connection postgresqlConnection;

    Control(Integer port, String host, String dbName, String username, Integer password) {
        this.port = port;
        this.host = host;
        this.dbName = dbName;
        this.username = username;
        this.password = password;
    }  
    public static void main(String[] args) {
        
    }
    
    public Connection getPostgresqlConnection(){
        return this.postgresqlConnection;
    }
    public boolean postgresqlConnect(String host, Integer port, String dbName, 
                                    String user, String password) {
        System.out.println("Test Postgresql");
        
        String url = "jdbc:postgresql://" + host + ":" + port + "/" + dbName;

        try {
            this.postgresqlConnection = DriverManager.getConnection(url, user,
                                                                    password);
            System.out.println("Connected to postgresql server.");
            return true;
        } catch(SQLException e) {
            System.out.println(e);
            return false;
        }
    } 

}
