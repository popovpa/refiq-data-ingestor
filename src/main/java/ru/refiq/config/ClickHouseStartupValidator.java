package ru.refiq.config;

import ru.refiq.error.FatalIngestException;
import ru.refiq.storage.ClickHouseClient;

public class ClickHouseStartupValidator {

    public void validate(DataIngestProperties.ClickHouse storage, ClickHouseClient client) {
        try {
            client.ping();
        } catch (RuntimeException e) {
            throw unavailable(e);
        }
        boolean databaseExists;
        try {
            databaseExists = client.databaseExists(storage.getDatabase());
        } catch (RuntimeException e) {
            throw unavailable(e);
        }
        if (!databaseExists) {
            throw new FatalIngestException("ClickHouse database does not exist: " + storage.getDatabase());
        }
        boolean tableExists;
        try {
            tableExists = client.tableExists(storage.getDatabase(), storage.getTable());
        } catch (RuntimeException e) {
            throw unavailable(e);
        }
        if (!tableExists) {
            throw new FatalIngestException("ClickHouse table does not exist: " + storage.getDatabase() + "." + storage.getTable());
        }
    }

    private static FatalIngestException unavailable(RuntimeException error) {
        if (error instanceof FatalIngestException fatal) {
            return fatal;
        }
        return new FatalIngestException("ClickHouse storage is not available", error);
    }
}
