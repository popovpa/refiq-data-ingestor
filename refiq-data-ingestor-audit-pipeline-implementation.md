# Audit pipeline в data-ingestor

Один Maven-модуль, один JAR (`ru.refiq.Bootstrap`), один Docker image. Clickstream и audit — два независимых процесса, выбранных через `data-ingest.source.type`. В одном JVM одновременно работает только один pipeline.

## Changes

Добавлены:

- `src/main/java/ru/refiq/config/ConditionalOnClickstreamPipeline.java`
- `src/main/java/ru/refiq/config/ConditionalOnAuditPipeline.java`
- `src/main/java/ru/refiq/config/IngestStorageStartupValidator.java`
- `src/main/java/ru/refiq/config/ClickHouseStorageStartupValidator.java`
- `src/main/java/ru/refiq/config/PostgresStorageStartupValidator.java`
- `src/main/java/ru/refiq/config/AuditDataSourceConfiguration.java`
- `src/main/java/ru/refiq/strategy/audit/AuditEvent.java`
- `src/main/java/ru/refiq/strategy/audit/AuditEventValidator.java`
- `src/main/java/ru/refiq/strategy/audit/AuditEventMapper.java`
- `src/main/java/ru/refiq/strategy/audit/AuditWriter.java`
- `src/main/java/ru/refiq/strategy/audit/AuditIngestStrategy.java`
- `src/main/resources/postgres/audit_logs.sql` (reference DDL, runtime его не выполняет)
- `src/test/java/ru/refiq/config/PipelineActivationTest.java`
- `src/test/java/ru/refiq/config/PostgresStorageStartupValidatorTest.java`
- `src/test/java/ru/refiq/strategy/audit/AuditEventMapperTest.java`
- `src/test/java/ru/refiq/strategy/audit/AuditWriterTest.java`
- `src/test/java/ru/refiq/strategy/audit/AuditIngestStrategyTest.java`

Изменены локально, без смены clickstream mapping / RowBinary / batch / offset / retry / DLQ / consumer loop:

- `pom.xml` — `spring-boot-starter-jdbc`, PostgreSQL driver `42.7.7`
- `Bootstrap.java` — исключены JDBC auto-configuration (`DataSource`, transaction manager, `JdbcTemplate`)
- `DataIngestProperties.java` — `storage.postgres`; `@Valid` снят с вложенных storage, чтобы чужой storage не был обязателен
- `IngestStartupChecker.java` — общие проверки, затем ровно один `IngestStorageStartupValidator`
- `IngestRuntimeConfiguration.java` — `Client` clickhouse только для clickstream
- Clickstream `@Component` и `ClickHouseHttpClient` — `@ConditionalOnClickstreamPipeline`
- `InfrastructureStartupTest.java` — новый конструктор checker
- `TestIngest.java` — `auditProperties()`

`application.yml` не менялся: дефолтный процесс остаётся clickstream и не содержит `storage.postgres`.

## Conditional architecture

Селектор — `data-ingest.source.type`.

- `clickstream` (и отсутствие свойства, `matchIfMissing = true`): создаются clickstream strategy, validator, mapper, enrichment, writer, `ClickHouseHttpClient`, ClickHouse `Client`, `ClickHouseStorageStartupValidator`. Audit strategy, `HikariDataSource`, `JdbcTemplate`, `PostgresStorageStartupValidator` не регистрируются.
- `audit`: создаются audit strategy, validator, mapper, writer, пул и `PostgresStorageStartupValidator`. Clickstream beans и ClickHouse client не регистрируются.

Это не «оба pipeline стартуют и один молчит». Условие стоит на типе бина, лишний бин не попадает в контекст.

`Bootstrap` исключает `org.springframework.boot.jdbc.autoconfigure.DataSourceAutoConfiguration`, `DataSourceTransactionManagerAutoConfiguration` и `JdbcTemplateAutoConfiguration`. Clickstream не читает `spring.datasource.*`. PostgreSQL datasource собирается вручную в `AuditDataSourceConfiguration` только при `source.type=audit`.

`IngestStartupChecker` всегда проверяет topic, type, group-id, batch, kafka, retry, monitoring port и наличие Kafka topic. Дальше вызывается единственный storage validator:

- clickstream → прежний `ClickHouseStartupValidator` (ping, database exists, table exists);
- audit → `SELECT 1` и строка в `information_schema.tables` для `audit_logs`. DDL при старте нет.

Чужой storage в эту проверку не входит. Недоступный ClickHouse не мешает audit, ненастроенный PostgreSQL не мешает clickstream.

## Runtime examples

Один и тот же artifact.

Clickstream:

```bash
java -jar data-ingestor.jar \
  --data-ingest.source.type=clickstream \
  --data-ingest.source.topic=clickstream-events \
  --data-ingest.source.group-id=data-ingest-clickstream \
  --data-ingest.storage.clickhouse.url=http://localhost:8123 \
  --data-ingest.storage.clickhouse.database=refiq \
  --data-ingest.storage.clickhouse.table=clickstream_events \
  --data-ingest.storage.clickhouse.username=refiq \
  --data-ingest.storage.clickhouse.password=refiq
```

```text
DATA_INGEST_SOURCE_TYPE=clickstream
DATA_INGEST_SOURCE_TOPIC=clickstream-events
DATA_INGEST_SOURCE_GROUP_ID=data-ingest-clickstream
DATA_INGEST_STORAGE_CLICKHOUSE_URL=http://localhost:8123
DATA_INGEST_STORAGE_CLICKHOUSE_DATABASE=refiq
DATA_INGEST_STORAGE_CLICKHOUSE_TABLE=clickstream_events
DATA_INGEST_STORAGE_CLICKHOUSE_USERNAME=refiq
DATA_INGEST_STORAGE_CLICKHOUSE_PASSWORD=refiq
```

Audit:

```bash
java -jar data-ingestor.jar \
  --data-ingest.source.type=audit \
  --data-ingest.source.topic=audit-events \
  --data-ingest.source.group-id=data-ingest-audit \
  --data-ingest.storage.postgres.url=jdbc:postgresql://localhost:5432/refiq \
  --data-ingest.storage.postgres.username=refiq \
  --data-ingest.storage.postgres.password=refiq
```

```text
DATA_INGEST_SOURCE_TYPE=audit
DATA_INGEST_SOURCE_TOPIC=audit-events
DATA_INGEST_SOURCE_GROUP_ID=data-ingest-audit
DATA_INGEST_STORAGE_POSTGRES_URL=jdbc:postgresql://localhost:5432/refiq
DATA_INGEST_STORAGE_POSTGRES_USERNAME=refiq
DATA_INGEST_STORAGE_POSTGRES_PASSWORD=refiq
```

`storage.postgres.table` по умолчанию `audit_logs`. Профили не нужны. Таблицу до старта audit-процесса создаёт внешний migration contour по `src/main/resources/postgres/audit_logs.sql`.

## Kafka

Подписка по-прежнему одна: `subscribe(List.of(source.topic))`, без pattern.

| Процесс | Topic | Consumer group | DLQ |
|---|---|---|---|
| clickstream | `clickstream-events` | `data-ingest-clickstream` | `clickstream-events.dlq` |
| audit | `audit-events` | `data-ingest-audit` | `audit-events.dlq` |

DLQ — существующее правило `<source-topic>.dlq`. Группы не пересекаются. Общих буферов, потоков и пулов между процессами нет: это разные JVM.

## PostgreSQL

Reference DDL, runtime его не исполняет:

```sql
CREATE TABLE IF NOT EXISTS audit_logs (
    id BIGSERIAL PRIMARY KEY,
    event_id VARCHAR(128) NOT NULL UNIQUE,
    schema_version INTEGER NOT NULL,
    event_type VARCHAR(128) NOT NULL,
    action VARCHAR(64) NOT NULL,
    occurred_at TIMESTAMPTZ NOT NULL,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    account_id VARCHAR(128),
    actor_type VARCHAR(64) NOT NULL,
    actor_id VARCHAR(128),
    entity_type VARCHAR(128) NOT NULL,
    entity_id VARCHAR(128) NOT NULL,
    request_id VARCHAR(128),
    correlation_id VARCHAR(128),
    ip_address VARCHAR(64),
    user_agent TEXT,
    changed_fields JSONB,
    before_data JSONB,
    after_data JSONB,
    metadata JSONB
);

CREATE INDEX IF NOT EXISTS audit_logs_occurred_at_idx
    ON audit_logs (occurred_at DESC);
CREATE INDEX IF NOT EXISTS audit_logs_entity_idx
    ON audit_logs (entity_type, entity_id, occurred_at DESC);
CREATE INDEX IF NOT EXISTS audit_logs_account_idx
    ON audit_logs (account_id, occurred_at DESC);
CREATE INDEX IF NOT EXISTS audit_logs_actor_idx
    ON audit_logs (actor_id, occurred_at DESC);
```

Отдельный индекс на `event_id` не дублируется: ограничение `UNIQUE` уже индекс. JSON `changes` пишется в колонку `changed_fields`.

`AuditIngestStrategy.type()` равен `audit`. Парсер не знает про Offer: entity type — строка (`ACCOUNT`, `USER`, `OFFER`, `PRODUCT`, …). `event_id` приходит от producer и внутри сервиса не генерируется. Обязательны `schemaVersion=1`, `eventId`, `eventType`, `occurredAt`, `actor.type`, `entity.type`, `entity.id`, `action`. `accountId` и `actor.id` могут отсутствовать.

## Idempotency

Доставка Kafka — at-least-once. Один `IngestBatch` пишется одной транзакцией: `setAutoCommit(false)`, `addBatch`, `executeBatch`, `commit`. SQL:

```sql
INSERT INTO audit_logs (...)
VALUES (..., CAST(? AS jsonb), ...)
ON CONFLICT (event_id) DO NOTHING
```

Повтор того же `event_id` не создаёт вторую строку и не считается ошибкой. Offset коммитится, потому что `strategy.process` завершается успешно. Rollback — при любой ошибке insert до commit.

## Error handling

Существующий контур, отдельной retry-системы нет.

- Невалидное событие (битый JSON, нет `eventId` / entity, чужой `schemaVersion`, плохой `occurredAt`, неизвестный `actor.type` или `action`) → `InvalidRecordException` → текущий DLQ `<topic>.dlq`. Валидные записи того же batch всё равно пишутся. После DLQ `process` возвращается нормально, offset коммитится по прежней семантике.
- Временный PostgreSQL (`08001` и прочие неклассифицированные как схема/constraint) → `RetryableIngestException`. Транзакция откатывается. `IngestSession` не вызывает `OffsetCommitter.commit`.
- Схема/constraint (`SQLState` `42*` и `23*`, в том числе отсутствие `audit_logs` на insert) → `FatalIngestException`.
- Старт audit: нет соединения или нет таблицы `audit_logs` → fail-fast до consumer. ClickHouse в эту проверку не входит.
- Старт clickstream: прежний fail-fast ClickHouse (URL, database, table, credentials, ping, databaseExists, tableExists). PostgreSQL не проверяется.

Readiness по-прежнему startup gate `InfrastructureReadiness`: consumer не стартует, пока не пройдены Kafka topic и свой storage. Отдельный custom readiness endpoint не добавлялся. Liveness — существующий Actuator. Метрики audit идут в общие счётчики `data.ingest.*` / `received.events` / `inserted.events` с тегом `type=audit`. Имена `clickhouse.*` не переименовывались.

## Tests

Добавлены:

- `PipelineActivationTest` — при `clickstream` есть clickstream/ClickHouse beans и нет audit/PostgreSQL; при `audit` наоборот; конфиг clickstream валиден без PostgreSQL; конфиг audit валиден без ClickHouse.
- `AuditEventMapperTest` — контракт v1, включая changes/before/after/metadata и SYSTEM без account/actor id.
- `AuditWriterTest` — batch insert, `ON CONFLICT (event_id) DO NOTHING`, одна транзакция; временная SQL-ошибка → retryable + rollback; `42P01` → fatal.
- `AuditIngestStrategyTest` — invalid → DLQ и не блокирует валидную запись; два одинаковых `event_id` оба уходят в writer; временный отказ PostgreSQL не коммитит offset.
- `PostgresStorageStartupValidatorTest` — таблица есть; таблицы нет; соединение недоступно; стартовая проверка только `SELECT`, без DDL.

`InfrastructureStartupTest` обновлён под список storage validators. Порядок clickstream startup тот же: topic → ping → databaseExists → tableExists → ready.

Полный прогон: `mvn test` — **73 tests, 0 failures, 0 errors, 0 skipped**.

## Regression check

Цепочка Kafka → data-ingestor-clickstream → ClickHouse функционально не менялась.

В clickstream-классах diff — только `@ConditionalOnClickstreamPipeline` и import. Не менялись `ClickstreamMapper`, порядок колонок `ClickstreamColumn`, RowBinary writer, `BatchAccumulator`, ручной commit в `IngestSession`, retry/rebalance/shutdown, DLQ publisher, цикл `IngestConsumerWorker`. Дефолт `application.yml` по-прежнему `source.type=clickstream` и `clickstream-events`. Прежние clickstream-тесты входят в те же 73 и прошли.

## Remaining work

В этой задаче не делалось и остаётся следующими этапами:

1. Transactional Outbox в `api`
2. публикация AuditEvent в Kafka
3. пилотный аудит Offer
4. Admin → Аудит
