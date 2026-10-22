# RefIQ `data-ingestor` — multi-process architecture audit

Дата аудита: 2026-10-03  
Режим: **READ-ONLY** (исходный код, конфигурация, Docker/CI/CD не изменялись)  
Файл: локальный артефакт аудита, не предназначен для commit.

---

## 1. Executive summary

```text
Готовность: PARTIALLY READY
```

Текущий `data-ingestor` уже спроектирован как **один runtime-процесс = один Kafka topic + один type + одна consumer group + один Strategy pipeline**. Это совпадает с целевым принципом изоляции процессов. Добавить второй процесс `audit` из того же JAR теоретически возможно через `data-ingest.source.*`, но **сейчас нельзя безопасно** из-за жёсткой привязки всего приложения к ClickHouse и безусловной регистрации clickstream-бинов.

Основные выводы:

1. **Kafka isolation уже почти готова**: один `IngestConsumerWorker`, `subscribe(List.of(topic))`, topic/group/type берутся из `DataIngestProperties`, нет `@KafkaListener`, нет pattern-subscription.
2. **Strategy pattern (`IngestStrategy` + `IngestStrategyRegistry`) — правильная точка расширения** для `AuditIngestStrategy` без общего event DTO.
3. **ClickHouse обязателен для любого процесса**: валидация в `IngestStartupChecker`, `@NotNull` storage.clickhouse, всегда создаются `ClickHouseHttpClient` и `com.clickhouse.client.api.Client`.
4. **Clickstream beans всегда стартуют** (`@Component` без `@Conditional` / `@Profile`): при `type=audit` процесс всё равно поднимет ClickHouse-клиенты и clickstream pipeline-классы.
5. **PostgreSQL / audit / profiles / Docker в репозитории отсутствуют.**
6. **Отдельные процессы дают естественную изоляцию failure/retry/DLQ/lag**, но только после decoupling storage и условной активации pipeline-бинов.
7. **Отдельный Audit microservice не требуется** текущим кодом: достаточно одного artifact + независимых runtime instances.
8. **Минимальный рефакторинг перед audit обязателен**; полный rewrite clickstream pipeline — нет.
9. **Observability частично готова**: Actuator health/prometheus + tag `type=<source.type>`, но нет Kafka lag metrics и pipeline-agnostic storage metrics.
10. **После REQUIRED-изменений** можно идти к Transactional Outbox + `data-ingestor-audit` без смены codebase/JAR-модели.

---

## 2. Current architecture

### 2.1 Maven / runtime skeleton

| Элемент | Факт |
|---|---|
| Project | single-module Maven, `ru.refiq:data-ingestor:1.0.0` |
| Java / Boot | Java 21, Spring Boot **4.1.0** |
| Entrypoint | `ru.refiq.Bootstrap` (`@SpringBootApplication`) |
| Packaging | Spring Boot fat JAR (`spring-boot-maven-plugin`) |
| Profiles | нет `application-*.yml`, нет `@Profile` |
| Docker / K8s / CI | в репозитории **нет** |
| Migrations | runtime DDL запрещён; DDL clickstream лежит как init-файл `src/main/resources/clickhouse/clickstream_events.sql` (не исполняется приложением) |

Зависимости runtime: `starter`, `actuator`, `webmvc`, `validation`, `kafka`, `clickhouse client-v2 0.9.8`, Micrometer Prometheus. JDBC/PostgreSQL/Flyway/Liquibase **нет**.

### 2.2 Package structure

```text
ru.refiq
├── Bootstrap
├── batch/                 # BatchAccumulator, IngestBatch, IngestRecord, FlushReason
├── config/                # properties, Kafka factories, startup validators, CH client bean
├── consumer/              # IngestConsumerWorker, IngestSession, RetryPolicy, OffsetCommitter
├── error/                 # exceptions, DeadLetter, KafkaDeadLetterPublisher
├── metrics/               # IngestMetrics, BufferGauge
├── storage/               # ClickHouseClient, ClickHouseHttpClient (ping/exists/metadata)
└── strategy/
    ├── IngestStrategy
    ├── IngestStrategyRegistry
    └── clickstream/       # validator/mapper/writer/columns/enrichment stubs
```

Общий transport/processing layer отделён от clickstream strategy. Storage layer пока = ClickHouse only.

### 2.3 Реальный runtime flow

```text
Bootstrap
  ↓
DataIngestProperties (prefix data-ingest)
  ↓
IngestStartupChecker (InitializingBean)
  ├ validate config
  ├ IngestStrategyRegistry.require(source.type)
  ├ KafkaTopicStartupValidator  → KafkaAdminTopicProbe.listTopics
  └ ClickHouseStartupValidator  → ping / databaseExists / tableExists
  ↓
InfrastructureReadiness.markReady()
  ↓
IngestConsumerWorker (SmartLifecycle)
  ├ createConsumer()
  ├ subscribe([data-ingest.source.topic])
  ├ poll → BatchAccumulator
  ├ flush (size | bytes | time)
  ├ IngestStrategy.process(IngestBatch)
  │    └ ClickstreamIngestStrategy
  │         ├ ClickstreamValidator.parse
  │         ├ ClickstreamMapper → ClickstreamEventRecord
  │         ├ ClickstreamEnrichmentPipeline (NoOp stubs)
  │         └ ClickstreamWriter → Client.insert(RowBinary)
  ├ commit offsets (manual, after persist)
  └ invalid → KafkaDeadLetterPublisher → <source-topic>.dlq
```

**Нет** `@KafkaListener`, `ConcurrentKafkaListenerContainerFactory`, `KafkaAdmin`/`NewTopic`, pattern subscribe.

### 2.4 Ключевые классы и конфигурации

| Слой | Классы / файлы |
|---|---|
| Config properties | `DataIngestProperties` + `application.yml` (`data-ingest.source/topic/type/group-id`, kafka, batch, retry, monitoring, storage.clickhouse) |
| Kafka consumer factory | `IngestKafkaConfiguration.ingestConsumerFactory` — `ByteArrayDeserializer`, `enable.auto.commit=false`, `allow.auto.create.topics=false`, `auto.offset.reset=earliest`, `group.id`/`client.id` из config |
| Kafka producer (DLQ) | `dlqProducerFactory` + `KafkaTemplate` |
| Consumer loop | `IngestConsumerWorker` (1 thread `data-ingest-consumer`) |
| Batch / offsets | `BatchAccumulator`, `IngestSession`, `OffsetCommitter` |
| Strategy selection | `IngestStrategyRegistry.require(type)` once per process |
| Clickstream | `ClickstreamIngestStrategy`, `ClickstreamMapper`, `ClickstreamWriter`, `ClickstreamColumn`, `ClickstreamEventRecord` |
| ClickHouse metadata | `ClickHouseHttpClient` (`SELECT` against `system`) |
| ClickHouse insert | `com.clickhouse.client.api.Client` bean from `IngestRuntimeConfiguration` |
| Startup gate | `IngestStartupChecker` → `InfrastructureReadiness` → worker.start() |
| Errors | `InvalidRecordException` → DLQ; `RetryableIngestException` → pause/backoff; `FatalIngestException` → exit |
| DLQ | `KafkaDeadLetterPublisher`, topic = `sourceTopic + ".dlq"` |
| Metrics | `IngestMetrics` (Micrometer), Actuator `/metrics` on `data-ingest.monitoring.port` |
| Health | Actuator `health` exposed; custom Kafka/CH readiness = fail-fast startup, не отдельный readiness probe endpoint |

### 2.5 Defaults в `application.yml`

```yaml
spring.application.name: data-ingestor
server.port: -1                 # business HTTP API нет
management.server.port: ${data-ingest.monitoring.port}  # default 9091
data-ingest.source.topic: clickstream-events
data-ingest.source.type: clickstream
data-ingest.source.group-id: data-ingest-clickstream
data-ingest.storage.clickhouse.*: required
```

Topic/group/type **не захардкожены в Java**, но defaults в YAML — clickstream-specific. Значения переопределяются Spring Boot property/env binding (`DATA_INGEST_SOURCE_TOPIC`, и т.п.).

---

## 3. Multi-process readiness

| Требование | Сейчас | Готово | Что требуется |
|---|---|---|---|
| Один JAR / один codebase | single Maven module, fat JAR | **Да** | — |
| 1 process = 1 topic | `subscribe(List.of(topic))` | **Да** | не добавлять multi-topic listeners |
| 1 process = 1 consumer group | `GROUP_ID_CONFIG` из `source.group-id` | **Да** | разные env на deployment |
| 1 process = 1 strategy | `registry.require(source.type)` | **Частично** | условная регистрация strategy beans; иначе audit-процесс поднимает clickstream beans |
| Запуск через profile/type | type есть; Spring profiles нет | **Частично** | достаточно `data-ingest.source.type` + conditional beans; profiles optional |
| Только свой pipeline активен | clickstream `@Component` всегда | **Нет** | `@ConditionalOnProperty(data-ingest.source.type=...)` или package/profile filtering |
| Независимость storage | ClickHouse always-on | **Нет** | storage abstraction + conditional CH/PG beans + startup validators |
| Независимый retry/DLQ lifecycle | process-local config + DLQ = `<topic>.dlq` | **Да** (на уровне процессов) | per-process config; не нужен shared handler |
| Независимость availability | CH down валит любой старт | **Нет** | не валидировать CH для audit; не валидировать PG для clickstream |
| Docker image × N deployments | Docker отсутствует в repo | **N/A / Да по модели** | внешний image/deployment может передавать разные env |
| Не мешать работающему clickstream | архитектура позволяет | **Да**, если REQUIRED-изменения локальные | не переписывать Batch/Session/Worker |

Эквивалент желаемого запуска уже близок:

```bash
java -jar data-ingestor.jar \
  --data-ingest.source.type=clickstream \
  --data-ingest.source.topic=clickstream-events \
  --data-ingest.source.group-id=data-ingest-clickstream
```

```bash
java -jar data-ingestor.jar \
  --data-ingest.source.type=audit \
  --data-ingest.source.topic=audit-events \
  --data-ingest.source.group-id=data-ingest-audit
```

Второй вариант **сегодня упадёт**: нет `IngestStrategy` для `audit`, и даже при его наличии процесс потребует живой ClickHouse.

---

## 4. Kafka isolation

### 4.1 Что уже соответствует модели `1 process = 1 topic = 1 group = 1 pipeline`

- Topic: `DataIngestProperties.Source.topic` → `IngestConsumerWorker.subscribe(List.of(...))`.
- Group: `ConsumerConfig.GROUP_ID_CONFIG = source.groupId`.
- Client id: `data-ingest-<type>` / DLQ `data-ingest-<type>-dlq`.
- Один consumer thread на процесс; concurrency Kafka listener container **не используется**.
- Deserialization: raw `byte[]`; schema parsing внутри Strategy (разные schemas возможны).
- Auto-create topics выключен; provisioning отсутствует.
- Manual commit only after durable write (`enable.auto.commit=false`).
- Rebalance handler в worker; pending batch не коммитится выше unpersisted offsets.
- Shared mutable state между consumers отсутствует: в процессе один consumer.

### 4.2 Проблемы / пробелы

| Проблема | Деталь |
|---|---|
| Defaults clickstream-only | YAML defaults topic/group/type; без override процесс всегда clickstream |
| Нет conditional consumers | Единственный worker всегда стартует после readiness; это ок для 1 pipeline/process, но storage/strategy beans не условные |
| DLQ topic naming fixed | `<sourceTopic>.dlq`; отдельный DLQ-конфиг/настройка отсутствует (для разных процессов всё равно разные topics) |
| Retry global per process | `data-ingest.retry.*` одно на процесс — для multi-process модели это **правильно**, не проблема |
| Offset policy hardcode | `auto.offset.reset=earliest` в `IngestKafkaConfiguration`; нельзя независимо менять без правки кода/конфига |
| Нет Kafka lag metric | lag нужно снимать внешне (Kafka exporter) или добавить позже |
| Strategy registry loads all strategies | `List<IngestStrategy>` инжектится целиком; при появлении audit без conditional оба Strategy-бина будут созданы в каждом процессе |

### 4.3 Hardcoded names

В Java **нет** hardcoded production topic/group для subscription.  
Hardcoded только:

- defaults в `application.yml`;
- DLQ suffix `.dlq`;
- clickstream strategy type literal `"clickstream"`;
- client-id prefix `data-ingest-`.

Это приемлемо для multi-process модели.

---

## 5. Storage isolation

### 5.1 Сейчас

```text
любой процесс
  ↓ обязательно
ClickHouseHttpClient + Client bean
  ↓ обязательно
ClickHouseStartupValidator (connection/db/table)
  ↓
только clickstream пишет через ClickstreamWriter
```

`DataIngestProperties.Storage` содержит только `ClickHouse` (`@NotNull`).  
`IngestStartupChecker.validate()` требует clickhouse url/database/table/username и `max.poll.interval > clickhouse.request-timeout`.

PostgreSQL, DataSource, JPA, JDBC starter — отсутствуют.

### 5.2 Риск при наивном добавлении PostgreSQL

Если добавить `spring-boot-starter-jdbc`/`data-jpa` и datasource без `@ConditionalOnProperty`, Spring Boot auto-config может:

- требовать `spring.datasource.*` для **всех** процессов;
- валить startup clickstream при недоступном Postgres health/auto-config;
- сделать оба storage обязательными.

Даже без Boot JDBC: если `IngestStartupChecker` начнёт всегда проверять и CH, и PG — оба процесса станут связаны availability.

### 5.3 Требование целевой модели

| Process | Должен поднимать | Не должен требовать |
|---|---|---|
| `data-ingestor-clickstream` | Kafka + ClickHouse | PostgreSQL |
| `data-ingestor-audit` | Kafka + PostgreSQL | ClickHouse |

**Сейчас это невозможно.** Нужна условная storage-конфигурация и storage-specific startup validation.

---

## 6. Failure isolation

### 6.1 Между процессами (целевое состояние)

Отдельные OS-процессы / Deployments с разными group/topic сами по себе дают:

- независимый consumer lag;
- независимый retry pause (`IngestSession` pause on storage outage);
- независимый DLQ topic;
- независимый fatal exit;
- отсутствие общего in-memory buffer.

Это сильная сторона текущей модели: **failure isolation = process isolation**, не in-process multi-pipeline.

### 6.2 Текущие дыры

1. **Любой процесс падает, если ClickHouse недоступен при старте** — audit невозможен.
2. При добавлении audit без conditional beans clickstream-класс `ClickstreamWriter` потребует CH `Client` даже в audit-процессе.
3. Clickstream backlog не влияет на audit **только если** они разные processes/groups — это уже так по дизайну, но audit ещё не существует.
4. Shared codebase не создаёт shared runtime state; риска cross-talk между replicas одного типа нет (стандартный Kafka partition assignment).

### 6.3 Duplicate processing / idempotency / shutdown

| Тема | Оценка |
|---|---|
| At-least-once | Да: commit только после persist |
| Duplicate on commit failure | Возможны; clickstream полагается на idempotency ключи/engine на стороне CH (Replacing/плоская схема без runtime DDL guarantee) |
| `client_event_id` | Сейчас Int64; legacy SDK string `[a-z0-9]{20}` → `0` (idempotency для legacy слабая) |
| Graceful shutdown | `SmartLifecycle` + `wakeup` + flush pending; timeout 25s join / Spring lifecycle 30s |
| Rebalance | pending/unpersisted offsets не коммитятся «вперёд» |
| Scaling replicas | Обычный Kafka consumer group; ок для одного type/process class |

---

## 7. Audit pipeline readiness

Целевой поток:

```text
API
 ↓
PostgreSQL TX (entity + outbox_event)
 ↓
Kafka topic audit-events
 ↓
data-ingestor-audit  (отдельный process)
 ↓
PostgreSQL audit_logs
```

### 7.1 Что можно переиспользовать без смешивания с clickstream

- `Bootstrap`, Kafka consumer factory pattern, `IngestConsumerWorker`, `IngestSession`, `BatchAccumulator`, retry/DLQ/error taxonomy, metrics plumbing, startup readiness gate.
- Новый `AuditIngestStrategy implements IngestStrategy` с `type() = "audit"`.
- Mapping `AuditEvent → audit_logs` — **внутри audit strategy/writer**, не в Kafka layer.
- Idempotency по `event_id`: natural key / upsert / `INSERT ... ON CONFLICT DO NOTHING` в AuditWriter (реализация storage-specific).

### 7.2 Что нельзя сделать «просто добавив класс»

1. Запустить audit-процесс без ClickHouse.
2. Не создавать clickstream beans в audit-процессе.
3. Иметь PostgreSQL-only startup validation.
4. Иметь storage metrics без clickhouse-specific naming (сейчас часть метрик `clickhouse.*`).

### 7.3 Нужно ли менять работающий clickstream pipeline?

**Нет, substantively.**  
Нужны точечные изменения вокруг lifecycle/conditions/storage binding:

- не ломать `ClickstreamIngestStrategy` / mapper / RowBinary writer;
- не менять offset semantics;
- не объединять consumers.

### 7.4 Отдельный microservice?

**Не требуется.** Текущий код не содержит причин, по которым audit обязан быть отдельным репозиторием/сервисом. Достаточно одного artifact и двух Deployments.

---

## 8. Required changes

### REQUIRED BEFORE AUDIT

1. **Условная активация pipeline beans по `data-ingest.source.type`**
   - Зачем: audit-процесс не должен создавать ClickstreamWriter/CH Client; clickstream-процесс не должен создавать AuditWriter/DataSource.
   - Классы: clickstream `@Component`s, будущие audit beans, возможно `IngestRuntimeConfiguration`.
   - Локальность: средняя; риск для clickstream низкий при `matchIfMissing`/`havingValue=clickstream`.
   - Риск clickstream: низкий, если default type остаётся `clickstream`.

2. **Сделать ClickHouse storage optional / type-scoped**
   - Зачем: audit не зависит от CH availability.
   - Классы: `DataIngestProperties.Storage`, `IngestStartupChecker`, `ClickHouseStartupValidator` wiring, `ClickHouseHttpClient`, `Client` bean.
   - Локальность: средняя.
   - Риск clickstream: средний (нужно сохранить текущий fail-fast для clickstream).

3. **Storage-specific startup validation**
   - Зачем: clickstream → Kafka topic + CH db/table; audit → Kafka topic + PG connectivity/schema prerequisites.
   - Классы: `IngestStartupChecker` или выделенные validators + strategy/type switch.
   - Локальность: локальная в config.
   - Риск clickstream: низкий при сохранении текущего CH check path.

4. **Добавить `AuditIngestStrategy` + audit model/writer (PostgreSQL)**
   - Зачем: собственно pipeline.
   - Классы: новый package `strategy.audit` (или аналог), PG repository/writer.
   - Локальность: additive.
   - Риск clickstream: минимальный, если conditional.

5. **Не подключать Boot DataSource auto-config глобально без conditions**
   - Зачем: иначе clickstream упадёт без Postgres.
   - Классы: future PG configuration.
   - Локальность: локальная для audit config.
   - Риск clickstream: высокий, если сделать неправильно; поэтому REQUIRED.

### RECOMMENDED

1. Метрика-тег `pipeline=${data-ingest.source.type}` явно (сейчас уже есть `type=...`) + `application=data-ingestor`.
2. Вынести clickhouse-specific metric names (`clickhouse.*`) за strategy/storage adapter, добавить generic `storage.write.*` для audit.
3. Сделать `auto.offset.reset` и DLQ topic template конфигурируемыми.
4. Profile files `application-clickstream.yml` / `application-audit.yml` как сахар над `source.type` (не обязательно).
5. Kafka consumer lag via Micrometer binder / external exporter.
6. Custom readiness endpoint, отражающий `InfrastructureReadiness` (сейчас fail-fast на старте уже есть).
7. Явный idempotency contract для audit `event_id` в writer tests.
8. Добавить файл в `.gitignore` для локальных audit-отчётов (опционально; этот файл не коммитить).

### NOT REQUIRED NOW

1. Отдельный Audit microservice / отдельный репозиторий.
2. Переход на `@KafkaListener` / listener container concurrency.
3. In-process multi-pipeline (оба consumers в одном JVM).
4. Transactional Outbox producer side внутри `data-ingestor` (outbox — ответственность API/DB writer).
5. Shared abstract “Event” DTO для всех типов.
6. Переписывание BatchAccumulator / offset state machine.
7. Distributed ID generator для clickstream.
8. GeoIP/ASN/attribution enrichment (остаются stubs).
9. Kubernetes/Docker манифесты в этом репозитории как блокер кода (их нет; могут жить снаружи).

---

## 9. Proposed target architecture

```text
один codebase / один JAR / один Docker image
                │
                ├─ Deployment: data-ingestor-clickstream
                │    env:
                │      DATA_INGEST_SOURCE_TYPE=clickstream
                │      DATA_INGEST_SOURCE_TOPIC=clickstream-events
                │      DATA_INGEST_SOURCE_GROUP_ID=data-ingest-clickstream
                │      DATA_INGEST_STORAGE_CLICKHOUSE_...
                │    beans:
                │      IngestConsumerWorker
                │      ClickstreamIngestStrategy
                │      ClickHouse* only
                │    flow:
                │      Kafka clickstream topic
                │           ↓
                │      data-ingestor-clickstream
                │           ↓
                │      ClickHouse clickstream_events
                │
                └─ Deployment: data-ingestor-audit
                     env:
                       DATA_INGEST_SOURCE_TYPE=audit
                       DATA_INGEST_SOURCE_TOPIC=audit-events
                       DATA_INGEST_SOURCE_GROUP_ID=data-ingest-audit
                       SPRING_DATASOURCE_... / DATA_INGEST_STORAGE_POSTGRES_...
                     beans:
                       IngestConsumerWorker
                       AuditIngestStrategy
                       PostgreSQL* only
                     flow:
                       Kafka audit topic
                            ↓
                       data-ingestor-audit
                            ↓
                       PostgreSQL audit_logs
```

Общими остаются:

- `Bootstrap`
- Kafka consumer loop / batch / retry / DLQ abstractions
- error taxonomy
- metrics/actuator plumbing
- strategy registry mechanism

Не общими в runtime:

- Strategy implementation
- Storage clients
- Startup validators
- Consumer group / topic / DLQ topic
- Retry clocks / buffers / lag

**Да: один codebase + один JAR + один Docker image при полностью независимых runtime processes — рекомендуемая и достижимая модель.**

Минимальное разделение ответственности (не обязательно физически такие имена):

```text
common:   batch, consumer, config skeleton, error, metrics
clickstream/: strategy + CH writer + enrichment
audit/:       strategy + PG writer + idempotency
```

---

## 10. Final verdict

1. **Можно ли сейчас безопасно добавить `audit` pipeline?**  
   **Нет.** Добавление audit-классов без decoupling ClickHouse/conditions сделает storage/startup связанными и/или будет создавать лишние beans.

2. **Нужно ли сначала рефакторить `data-ingestor`?**  
   **Да, точечно (REQUIRED BEFORE AUDIT).** Не нужен rewrite сервиса.

3. **Какие минимальные изменения обязательны?**  
   - conditional beans по `data-ingest.source.type`;  
   - ClickHouse обязателен только для clickstream;  
   - PostgreSQL (будущий) обязателен только для audit;  
   - type-specific startup validation;  
   - новый `AuditIngestStrategy` + PG writer с idempotency по `event_id`.

4. **Нужно ли менять уже работающий clickstream pipeline?**  
   **Нет по бизнес-логике/mapping/insert.** Нужны только обвязка conditions/storage binding вокруг него.

5. **Можно ли обеспечить полную runtime-изоляцию из одного artifact?**  
   **Да**, это уже заложено моделью «1 process / 1 topic / 1 group / 1 strategy»; блокирует только always-on ClickHouse + unconditional clickstream components.

6. **Можно ли после необходимых изменений перейти к Transactional Outbox + Audit Consumer?**  
   **Да.** Outbox остаётся на стороне API/PostgreSQL producer. `data-ingestor-audit` становится обычным isolated consumer того же каркаса, без отдельного microservice и без совместного runtime с clickstream.

---

## Appendix A — Deployment readiness (без изменения deployment)

Мешает сейчас:

- обязательный ClickHouse config/startup;
- отсутствие audit strategy;
- отсутствие PostgreSQL support;
- отсутствие Docker/K8s манифестов в repo (не блокер кода; образ можно собирать снаружи);
- нет готовых profile-файлов, но env/`--data-ingest.*` достаточно после REQUIRED-фикса.

Не мешает:

- один fat JAR;
- конфигурируемые topic/group/type;
- отдельный management port;
- graceful shutdown;
- отсутствие business HTTP API (`server.port: -1`).

Пример целевых env (иллюстративно):

```text
# clickstream
DATA_INGEST_SOURCE_TYPE=clickstream
DATA_INGEST_SOURCE_TOPIC=clickstream-events
DATA_INGEST_SOURCE_GROUP_ID=data-ingest-clickstream
DATA_INGEST_STORAGE_CLICKHOUSE_URL=...
DATA_INGEST_MONITORING_PORT=9091

# audit
DATA_INGEST_SOURCE_TYPE=audit
DATA_INGEST_SOURCE_TOPIC=audit-events
DATA_INGEST_SOURCE_GROUP_ID=data-ingest-audit
SPRING_DATASOURCE_URL=...
DATA_INGEST_MONITORING_PORT=9092
```

## Appendix B — Observability checklist

| Signal | Сейчас | Для multi-process |
|---|---|---|
| health | Actuator `/metrics/health` | ок; желательно не требовать чужой storage |
| readiness/liveness | startup fail-fast + Actuator health | разделить liveness (process up) vs readiness (Kafka+own storage) |
| Kafka lag | нет in-app | exporter или binder |
| processing errors / retries / DLQ | counters с tag `type` | ок |
| throughput / latency | batch/storage timers | ок; добавить audit storage timer |
| storage write errors | `data.ingest.storage.write.errors` | ок |
| pipeline distinction | tag `type=<source.type>`, `application=data-ingestor` | достаточно; можно добавить `pipeline` alias |

## Appendix C — Architectural smells found

- ClickHouse hard-wired into global startup and properties.
- Unconditional `@Component` scan для всего clickstream package.
- `IngestStartupChecker` смешивает generic config validation и CH-specific checks.
- Часть метрик названа `clickhouse.*` на общем `IngestMetrics`.
- Нет extension point «storage validator per type».
- Defaults YAML clickstream-centric (приемлемо, но усиливает связность).
- Нет `@Conditional` / `@Profile` инфраструктуры для multi-pipeline artifact.

Главный принцип подтверждён кодом как цель и частично реализован:

> Один тип Kafka-данных из одного topic читается, обрабатывается и сохраняется отдельным изолированным процессом `data-ingestor`. Общими могут быть codebase и artifact, но не runtime pipeline.

До состояния READY не хватает в основном **conditional pipeline/storage activation**.
