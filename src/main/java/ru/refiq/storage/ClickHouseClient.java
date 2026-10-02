package ru.refiq.storage;

public interface ClickHouseClient {

    void ping();

    boolean databaseExists(String database);

    boolean tableExists(String database, String table);

    void insertJsonEachRow(String insertSql, String jsonEachRowBody);
}
