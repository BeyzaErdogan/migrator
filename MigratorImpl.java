package com.inscada.migrator;

import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.BatchPoints;
import org.influxdb.dto.Point;

public class MigratorImpl implements Migrator {
    private final App app;

    private InfluxDB influxdbConnection;
    private Connection postgresqlConnection;
    
    List<Integer> arrays = new ArrayList<>();

    int i = 0;

    private int counter_e = 1;
    private int counter_f = 1;
    private int counter_v = 1;

    MigratorImpl(App app) {
        this.app = app;
    }

    public InfluxDB getInfluxdbConnection() {
        return influxdbConnection;
    }

    public Connection getPostgresqlConnection() {
        return postgresqlConnection;
    }

    @Override
    public boolean testPostgresqlConnection(ConnectionInfo postgresqlConnectionInfo) {
        String url = "jdbc:postgresql://" + postgresqlConnectionInfo.getHost() + ":" + postgresqlConnectionInfo.getPort()
                + "/" + postgresqlConnectionInfo.getDbname();
        
        try {
            this.postgresqlConnection = DriverManager.getConnection(url, postgresqlConnectionInfo.getUsername(),
                    postgresqlConnectionInfo.getPassword().toString());
            System.out.println("Connected to PostgreSQL.");
            return true;
        } catch (SQLException e) {
            System.out.println(e);
            return false;
        }
    }

    @Override
    public boolean testInfluxDbConnection(ConnectionInfo influxdbcConnectionInfo) {
        String url = "http://" + influxdbcConnectionInfo.getHost() + ":" + influxdbcConnectionInfo.getPort();

        try {
            this.influxdbConnection = InfluxDBFactory.connect(url);
            System.out.println("Connected to InfluxDB.");
            influxdbConnection.enableBatch();
            return true;
        } catch (Exception e) {
            System.out.println("Not Connected.");
            return false;
        }
    }

    public String getVariableName(Integer variableId) {
        String variableName = null;
        Map<Integer, String> variableIdNameMap = new ConcurrentHashMap<>();

        if (variableIdNameMap.containsKey(variableId)) {
            variableName = variableIdNameMap.get(variableId);
        } else {
            try {
                Statement st = postgresqlConnection.createStatement();
                ResultSet rs = st.executeQuery("SELECT variable_name FROM variable WHERE v_id = " + variableId);
                while (rs.next()) {
                    variableName = rs.getString(1);
                }
                variableIdNameMap.put(variableId, variableName);
            } catch (SQLException e) {
                System.out.println(e);
            }
        }
        return variableName;
    }

    public String getProjectName(Integer projectId) {
        String projectName = null;
        ConcurrentHashMap<Integer, String> projectIdNameMap = new ConcurrentHashMap<>();

        if (projectIdNameMap.containsKey(projectId)) {
            projectName = projectIdNameMap.get(projectId);
        } else {
            try {
                Statement st = postgresqlConnection.createStatement();
                ResultSet rs = st.executeQuery("SELECT project_name"
                        + " FROM project"
                        + " WHERE p_id = "
                        + projectId);
                while (rs.next()) {
                    projectName = rs.getString(1);
                }
                projectIdNameMap.put(projectId, projectName);
            } catch (SQLException e) {
                System.out.println(e);
            }
        }
        return projectName;
    }

    public void counter(ExecutorService executorService, final String tableName, final int offset, final int limit,
                            int Threads, final String startTime, final String endTime, final int count) {
        executorService.submit(new Runnable() {
            
            @Override
            public void run() {
                if (tableName.equals("fired_alarm")) {
                    transferFiredAlarms(offset, limit, startTime, endTime,count);
                } else if (tableName.equals("event_log")) {
                    transferEventLogs(offset, limit, startTime, endTime,count);
                } else if (tableName.equals("read_variable_num")) {
                    transferVariableValues(limit, offset, startTime, endTime,count);
                } else {
                    System.out.println("Not Succesful");
                }
            }
        });
    }

    @Override
    public void threadProduce(int Threads, int BatchSize, final String tableName, String timestamp, final String startTime, 
                              final String endTime) {
        final int batch = BatchSize;
        int count = 0, limit = batch, offset = 0;
        
        count = Count(tableName, timestamp, startTime, endTime);

        int temp = count / batch;
        int stayedtmp = count % batch; 

        ExecutorService executorService = Executors.newFixedThreadPool(Threads);
        
        if (tableName.equals("read_variable_num")) {
            getAllProjectId(count);
        }
        for (int i = 0; i < temp-1; i++) {
            counter(executorService, tableName, offset, limit, Threads, startTime, endTime,count);
            offset += batch;
        }
        limit=batch+stayedtmp;
        counter(executorService, tableName, offset, limit, Threads, startTime, endTime,count);
        
    }

    private int Count(String tableName, String timestamp, String startDate, String endDate) {
        int count = 0;
        
        try {
            Statement st = postgresqlConnection.createStatement();
            String format = String.format("SELECT COUNT(*) FROM %s WHERE %s BETWEEN '%s' AND '%s' ", tableName, timestamp, startDate, endDate);
            ResultSet rs = st.executeQuery(format);
            while (rs.next()) {
                count = rs.getInt(1);
            }
        } catch (SQLException ex) {
            Logger.getLogger(MigratorImpl.class.getName()).log(Level.SEVERE, null, ex);
        }
        return count;
    }
    
    public void getAllProjectId(int count) {
        int projectId = 0;
        
        try {
            Statement st = postgresqlConnection.createStatement();
            String format = String.format("SELECT * FROM project  LIMIT %d ", count);
            ResultSet rs = st.executeQuery(format);

            while (rs.next()) {
                projectId = rs.getInt(2);
                arrays.add(projectId);
            }
        } catch (SQLException ex) {
            Logger.getLogger(MigratorImpl.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
    public void progressCntrl(int progress){
        
        if (progress==100) {
            app.transferEnabled();
        }
    }

    @Override
    public void transferEventLogs(int offset, int limit, final String startE, final String endE, int count) {
        int progress = 0;
        List<Point> e_points = new ArrayList<>();
        
        try {
            Statement st = postgresqlConnection.createStatement();
            String query = String.format("SELECT * FROM event_log  WHERE dttm BETWEEN '%s' AND '%s' ORDER BY dttm LIMIT %d OFFSET %d ", startE, endE, limit, offset);
            ResultSet rs = st.executeQuery(query);

            while (rs.next()) {
                counter_e++;
                progress = (int) (100.0 * counter_e / count);
                app.setProgress(progress);

                Integer projectId = rs.getInt(6);
                String projectName = getProjectName(projectId);
                String activity = rs.getString(1);
                String msg = rs.getString(2);
                Timestamp ts = rs.getTimestamp(3);
                String log_severity = rs.getString(4);

                Map<String, String> tags = new HashMap<String, String>();
                tags.put("project", projectName);
                tags.put("activity", activity);
                tags.put("severity", log_severity);
                Point point = Point.measurement("event_log")
                        .time(ts.getTime(), TimeUnit.MILLISECONDS)
                        .tag(tags).addField("msg", msg).build();
                e_points.add(point);
            }
            BatchPoints batchPoints = BatchPoints
                    .database("inscada")
                    .retentionPolicy("event_log_rp")
                    .points(e_points)
                    .build();
            influxdbConnection.write(batchPoints);
            System.out.println("Batch written.");
            System.out.println("Data is written.");
        } catch (SQLException e) {
            System.out.println(e);
        }
        progressCntrl(progress);
    }

    @Override
    public void transferFiredAlarms(int offset, int limit, String startF, String endF, int count) {
        int progress = 0;
        List<Point> f_points = new ArrayList<>();
        
        try {
            Statement st = postgresqlConnection.createStatement();
            String query = String.format("SELECT * FROM fired_alarm WHERE on_dttm BETWEEN '%s' AND '%s' ORDER BY on_dttm LIMIT %d OFFSET %d", startF, endF, limit, offset);
            ResultSet rs = st.executeQuery(query);
            
            while (rs.next()) {
                counter_f++;
                progress = (int) (100.0 * counter_f / count);
                app.setProgress3(progress);

                Integer alarmId = rs.getInt(2);
                Integer projectId = rs.getInt(3);
                String projectName = getProjectName(projectId);
                String status = rs.getString(4);
                Timestamp onDttm = rs.getTimestamp(5);
                Timestamp offDttm = rs.getTimestamp(6);
                Timestamp acknowledge = rs.getTimestamp(7);
                String acknowledger = rs.getString(8);

                Map<String, String> tags = new HashMap<String, String>();
                tags.put("project_id", projectId.toString());
                tags.put("project", projectName);
                tags.put("alarm_id", alarmId.toString());

                Point point = Point.measurement("fired_alarm")
                        .time(onDttm.getTime(), TimeUnit.MILLISECONDS)
                        .tag(tags).addField("status", status).addField("off_time", offDttm.toString())
                        .addField("ack_time", acknowledge.toString())
                        .addField("ack_by", acknowledger).build();
                f_points.add(point);
            }
            BatchPoints batchPoints = BatchPoints
                    .database("inscada")
                    .retentionPolicy("fired_alarm_rp")
                    .points(f_points)
                    .build();
            influxdbConnection.write(batchPoints);
            System.out.println("Batch written.");
            System.out.println("Data is written.");
        } catch (SQLException ex) {
            Logger.getLogger(MigratorImpl.class.getName()).log(Level.SEVERE, null, ex);
        }
        progressCntrl(progress);
    }

    @Override
    public void transferVariableValues(int limit, int offset, String startV, String endV, int count) {
        int progress = 0;
        List<Point> r_points = new ArrayList<>();

        try {
            Statement st = postgresqlConnection.createStatement();
            String query = String.format("SELECT * FROM read_variable_num WHERE read_dttm BETWEEN '%s' AND '%s' ORDER BY read_dttm LIMIT %d OFFSET %d ", startV, endV, limit, offset);
            ResultSet rs = st.executeQuery(query);

            while (rs.next()) {
                counter_v++;
                progress = (int) (100.0 * counter_v / count);
                app.setProgress2(progress);
                
                Integer projectId = arrays.get(i);
                i++;
                
                String projectName = getProjectName(projectId);
                Integer variableId = rs.getInt(1);
                String name = getVariableName(variableId);
                Timestamp time = rs.getTimestamp(2);
                Float value = rs.getFloat(3);

                Map<String, String> tags = new HashMap<String, String>();
                tags.put("project_id", projectId.toString());
                tags.put("project", projectName);
                tags.put("variable_id", variableId.toString());
                tags.put("name", name);

                Point point = Point.measurement("variable_value")
                        .time(time.getTime(), TimeUnit.MILLISECONDS)
                        .tag(tags).addField("value", value).build();
                r_points.add(point);
            }
            BatchPoints batchPoints = BatchPoints
                    .database("inscada")
                    .retentionPolicy("variable_value_rp")
                    .points(r_points)
                    .build();
            influxdbConnection.write(batchPoints);
            System.out.println("Batch written.");
            System.out.println("Data is written.");
        } catch (SQLException ex) {
            Logger.getLogger(MigratorImpl.class.getName()).log(Level.SEVERE, null, ex);
        }
        progressCntrl(progress);
    }
}
