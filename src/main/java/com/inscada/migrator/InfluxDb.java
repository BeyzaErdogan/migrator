
package com.inscada.migrator;

import java.sql.Connection;
import java.sql.*;

public class InfluxDb {
    private InfluxDb influxdbConnection;
    public static void main(String[] args) {
        InfluxDb a = new InfluxDb();
//        a.influxdbConnect()
/*
 String databaseURL,userName,password;
 //Connection connect;
 InfluxDb influxDB = InfluxDBFactory.connect(databaseURL, userName, password);        
   */     
    }
    private InfluxDb InfluxDBFactory;
    private Object influxDB;
    public boolean influxdbConnect(String host, Integer port) {
        Connection connect;
        try {
            String url = "http://" + host + ":" + port;
           influxdbConnection = InfluxDBFactory.connect(url);
            System.out.println("CONNECTED");
            return true;
        } catch(Exception e) {
            System.out.println("NOT CONNECTED");
            return false;
        }
 
    }

    private InfluxDb connect(String url) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }
}