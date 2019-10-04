package com.inscada.migrator;

/**
 *
 * @author fadime
 */
public interface Migrator {

    void transferEventLogs(int offset, int limit, String startE, String endE, int count);

    void transferFiredAlarms(int limit, int offset, String startF, String endF, int count);

    void transferVariableValues(int limit, int offset, String startV, String endV, int count);

    boolean testPostgresqlConnection(ConnectionInfo connectionInfo);

    boolean testInfluxDbConnection(ConnectionInfo connectionInfo);

    public void threadProduce(int nThreads, int nBatchSize, String tableName, String timestamp, String startTime, String endTime);
}
