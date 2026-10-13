# PostgreSQL → Fraud REST API → Kafka Connect Source

Lab project targeting Java 8, Kafka Connect API 3.4.1 / Confluent Platform 7.4.x.

## Build

```bash
cd fraud-api && javac FraudApi.java && java FraudApi 8099
cd ../connector && mvn clean package
```

The shaded JAR at `connector/target/postgres-fraud-source-1.0.0.jar` includes PostgreSQL JDBC driver; Kafka Connect API is provided by worker.

## Setup

Create database user first, then run `config/setup.sql` as database admin. Change `POSTGRES_HOST`, password and API host in `config/connector.json`. Install the JAR into a dedicated plugin directory on *both* Connect workers, include parent directory in `plugin.path`, and restart workers one at a time.

Register via `curl -X POST -H 'Content-Type: application/json' --data @config/connector.json http://cpkafka5:8083/connectors`.

## Important limits

* This is INSERT-only JDBC incremental polling by monotonically increasing ID, **not** WAL CDC. Updates/deletes and late rows with smaller IDs are not captured.
* Source offsets are asynchronous. Restart can cause duplicates. Deduplicate by transaction ID downstream.
* HTTP API has no authentication/TLS; do not expose it outside a trusted lab.
* The API's amount threshold is a demo rule, not production fraud detection.
* One task is supported; `tasks.max` does not parallelize it.
* JSON is sent as a StringConverter payload; consumers see the JSON string as plain text.
* No transactional coordination exists between DB, API and Kafka; API validation can repeat.
* This sample's `lastId` in memory advances before Kafka offset commits. If a poll returns some records then encounters a failed row, those records are returned and will be committed asynchronously; a crash may replay them.
