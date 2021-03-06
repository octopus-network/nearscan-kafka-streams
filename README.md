
# indexer
```
https://github.com/near/near-indexer-for-explorer

public生产的java文件public$
schema: public -> indexer (avro generated file: near.mainnet.public$.action_receipt_actions)


DATABASE_URL="postgres://username:password@34.85.112.76/db_mainnet?options=-c search_path%3Dindexer"

注释 diesel.toml [print_schema]
M	migrations/2020-12-07-153402_initial_schema/down.sql
M	migrations/2020-12-07-153402_initial_schema/up.sql
M	migrations/2021-03-29-183641-readonly_role/up.sql
M	migrations/2021-06-09-102523_grant_select_on_new_tables/down.sql
M	migrations/2021-06-09-102523_grant_select_on_new_tables/up.sql

```

# cdc
```
{
  "name": "PostgresCdcSourceConnector_0",
  "config": {
    "connector.class": "PostgresCdcSource",
    "name": "PostgresCdcSourceConnector_0",
    "kafka.api.key": "ESO73WMN5LRW26BI",
    "kafka.api.secret": "pDjYSFFZU6rAU/odZ8n4IQEpbiD3RfyT8YV/OYvDtOUkntyg6iwOOLWUlkbKgJBi",
    "database.hostname": "34.85.112.76",
    "database.port": "5432",
    "database.user": "username",
    "database.password": "password",
    "database.dbname": "db_mainnet",
    "database.server.name": "near",
    "database.sslmode": "disable",
    "table.include.list": "indexer.receipts, indexer.action_receipt_actions, indexer.execution_outcomes, indexer.heartbeat",
    "slot.name": "debezium_testnet",
    "heartbeat.interval.ms": "60000",
    "heartbeat.action.query": "INSERT INTO indexer.heartbeat (id, ts) VALUES (1, NOW()) ON CONFLICT(id) DO UPDATE SET ts=EXCLUDED.ts",
    "output.data.format": "AVRO",
    "tasks.max": "1",
    "transforms": "ExtractReceiptID,ExtractExecutedReceiptID ",
    "predicates": "IsReceipt,IsExecutedReceipt ",
    "transforms.ExtractReceiptID.type": "org.apache.kafka.connect.transforms.ExtractField$Key",
    "transforms.ExtractReceiptID.field": "receipt_id",
    "transforms.ExtractReceiptID.predicate": "IsReceipt",
    "transforms.ExtractExecutedReceiptID.type": "org.apache.kafka.connect.transforms.ExtractField$Key",
    "transforms.ExtractExecutedReceiptID.field": "executed_receipt_id",
    "transforms.ExtractExecutedReceiptID.predicate": "IsExecutedReceipt",
    "predicates.IsReceipt.type": "org.apache.kafka.connect.transforms.predicates.TopicNameMatches",
    "predicates.IsReceipt.pattern": ".*receipt.*(?<!execution_outcome_receipts)$|.*execution_outcomes",
    "predicates.IsExecutedReceipt.type": "org.apache.kafka.connect.transforms.predicates.TopicNameMatches",
    "predicates.IsExecutedReceipt.pattern": ".*execution_outcome_receipts"
  }
}
```

```
transforms=ExtractBlockHash,ExtractChunkHash,ExtractTransactionHash,ExtractReceiptID,ExtractExecutedReceiptID

transforms.ExtractBlockHash.type=org.apache.kafka.connect.transforms.ExtractField$Key
transforms.ExtractBlockHash.field=block_hash
transforms.ExtractBlockHash.predicate=IsBlock

transforms.ExtractChunkHash.type=org.apache.kafka.connect.transforms.ExtractField$Key
transforms.ExtractChunkHash.field=chunk_hash
transforms.ExtractChunkHash.predicate=IsChunk

transforms.ExtractTransactionHash.type=org.apache.kafka.connect.transforms.ExtractField$Key
transforms.ExtractTransactionHash.field=transaction_hash
transforms.ExtractTransactionHash.predicate=IsTransaction

transforms.ExtractReceiptID.type=org.apache.kafka.connect.transforms.ExtractField$Key
transforms.ExtractReceiptID.field=receipt_id
transforms.ExtractReceiptID.predicate=IsReceipt

transforms.ExtractExecutedReceiptID.type=org.apache.kafka.connect.transforms.ExtractField$Key
transforms.ExtractExecutedReceiptID.field=executed_receipt_id
transforms.ExtractExecutedReceiptID.predicate=IsExecutedReceipt
```
```
predicates=IsBlock,IsChunk,IsTransaction,IsReceipt,IsExecutedReceipt

predicates.IsBlock.type=org.apache.kafka.connect.transforms.predicates.TopicNameMatches
predicates.IsBlock.pattern=.*blocks

predicates.IsChunk.type=org.apache.kafka.connect.transforms.predicates.TopicNameMatches
predicates.IsChunk.pattern=.*chunks

predicates.IsTransaction.type=org.apache.kafka.connect.transforms.predicates.TopicNameMatches
predicates.IsTransaction.pattern=.*transaction.*

predicates.IsReceipt.type=org.apache.kafka.connect.transforms.predicates.TopicNameMatches
predicates.IsReceipt.pattern=.*receipt.*(?<!execution_outcome_receipts)$|.*execution_outcomes

predicates.IsExecutedReceipt.type=org.apache.kafka.connect.transforms.predicates.TopicNameMatches
predicates.IsExecutedReceipt.pattern=.*execution_outcome_receipts
```

```
transforms=CopyAccountID,ExtractAccountID
transforms.CopyAccountID.type=org.apache.kafka.connect.transforms.ValueToKey
transforms.CopyAccountID.fields=account_id
transforms.CopyAccountID.predicate=IsAccount

transforms.ExtractAccountID.type=org.apache.kafka.connect.transforms.ExtractField$Key
transforms.ExtractAccountID.field=account_id
transforms.ExtractAccountID.predicate=IsAccount

predicates=IsAccount
predicates.IsAccount.type=org.apache.kafka.connect.transforms.predicates.TopicNameMatches
predicates.IsAccount.pattern=.*accounts
```

```
transforms=ExtractAccessKey
transforms.ExtractAccessKey.type=org.apache.kafka.connect.transforms.ExtractField$Key
transforms.ExtractAccessKey.field=account_id
transforms.ExtractAccessKey.predicate=IsAccessKey

predicates=IsAccessKey
predicates.IsAccessKey.type=org.apache.kafka.connect.transforms.predicates.TopicNameMatches
predicates.IsAccessKey.pattern=.*access_keys
```

```
transforms=CopyAccountChangeID,ExtractAccountChangeID
transforms.CopyAccountChangeID.type=org.apache.kafka.connect.transforms.ValueToKey
transforms.CopyAccountChangeID.fields=affected_account_id
transforms.CopyAccountChangeID.predicate=IsAccountChange

transforms.ExtractAccountChangeID.type=org.apache.kafka.connect.transforms.ExtractField$Key
transforms.ExtractAccountChangeID.field=affected_account_id
transforms.ExtractAccountChangeID.predicate=IsAccountChange

predicates=IsAccountChange
predicates.IsAccountChange.type=org.apache.kafka.connect.transforms.predicates.TopicNameMatches
predicates.IsAccountChange.pattern=.*account_changes
```

# sql
```
select * from pg_replication_slots;

select pg_drop_replication_slot('test_cdc');

SELECT slot_name,
  pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)) as replicationSlotLag,
  pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn)) as confirmedLag,
  active
FROM pg_replication_slots;
```

# maven
```
mvn help:evaluate ${}

mvn clean

mvn schema-registry:download

mvn schema-registry:register

mvn generate-sources

mvn compile jib:buildTar -U -e -X

mvn package

java -cp target/nearin-7.0.0-standalone.jar \
  network.octopus.nearin.TokenBalance \
  src/main/resources/config/dev.properties


mvn compile jib:dockerBuild

docker run -it --rm -p 7071:7071 -p 7072:7072 \
  -v $(pwd)/src/main/resources/config/dev.properties:/nearin/config.properties \
  -v $(pwd)/tmp:/tmp
  token-balance:0.0.1

curl 127.0.0.1:7071/
curl 127.0.0.1:7072/jolokia/
```

```
cd /Users/deallinker-ry/Desktop/ni/nearin ; /usr/bin/env /Library/Java/JavaVirtualMachines/jdk-17.0.1.jdk/Contents/Home/bin/java -XX:+ShowCodeDetailsInExceptionMessages -Dfile.encoding=UTF-8 @/var/folders/p9/09yh0y_s47s3xb96dbnjxqfw0000gn/T/cp_4iswtlop3w2o61bqbe7pcub8x.argfile network.octopus.nearin.TokenBalance
```


# reset
```
output topic:
- nearin.oct_balance
- nearin.oct_transfer


./kafka-streams-application-reset \
  --bootstrap-servers pkc-43n10.us-central1.gcp.confluent.cloud:9092 \
  --application-id octopus-balance \
  --config-file ~/.confluent/java.config \
  --input-topics near.indexer.receipts,near.indexer.execution_outcomes,near.indexer.action_receipt_actions \
  --to-earliest --force


./kafka-avro-console-consumer \
  --topic nearin.oct_balance \
  --from-beginning \
  --bootstrap-server pkc-43n10.us-central1.gcp.confluent.cloud:9092 \
  --consumer.config /Users/ruanyu/Desktop/ni/nearin/src/main/resources/config/ccloud-dev.properties  \
  --property print.key=flase \
  --property print.offset=true \
  --property schema.registry.url=https://psrc-2225o.us-central1.gcp.confluent.cloud \
  --property basic.auth.credentials.source=USER_INFO \
  --property schema.registry.basic.auth.user.info='UGUW77JNIJZLCQMP:NmhDelI8DR3AoFeSX+0GijwBOgFy0MTfdAgGDMq+L+hKmXB3xQh0tIZhsHtMKV+2'

```

# example message
```
"H1jkXHMtBr17bhyKH1JaTJZpXYtxFwjS6ngk51NE5y35", {
  "before": null,
  "after": {
    "receipt_id": "H1jkXHMtBr17bhyKH1JaTJZpXYtxFwjS6ngk51NE5y35",
    "included_in_block_hash": "59Gz53qY75gmJAjHAYivtAzHJ5VqB9pby63cbghmAYZ4",
    "included_in_chunk_hash": "D1Dd4bYp3Ld3Ht628mZ6guzSNNdeBJ7z9GSpgAkTtcB4",
    "index_in_chunk": 7,
    "included_in_block_timestamp": 1628737958947945772,
    "predecessor_account_id": "f5cfbc74057c610c8ef151a439252680ac68c6dc.factory.bridge.near",
    "receiver_account_id": "f5cfbc74057c610c8ef151a439252680ac68c6dc.factory.bridge.near",
    "receipt_kind": "ACTION",
    "originated_from_transaction_hash": "9nmMgdiWs6k46pUJAqvbkH8bQvnbkeJ2eJRr8qpKo6Hm"
  },
  "source": {
    "version": "1.3.1.Final",
    "connector": "postgresql",
    "name": "test",
    "ts_ms": 1637127365178,
    "snapshot": "true",
    "db": "db_mainnet",
    "schema": "indexer",
    "table": "receipts",
    "txId": 494462,
    "lsn": 3104780760,
    "xmin": null
  },
  "op": "r",
  "ts_ms": 1637127365178,
  "transaction": null
}
```


# jmx
[Monitoring Kafka Streams Applications](https://docs.confluent.io/platform/current/streams/monitoring.html#built-in-metrics)
[jmx_exporter](https://github.com/prometheus/jmx_exporter)
[dashboard](https://grafana.com/grafana/dashboards/13966)
[!!!Kafka Summit Europe 2021](https://github.com/nbuesing/kafka-streams-dashboards/search?q=lowercaseOutputName)
```
-javaagent:./jmx_prometheus_javaagent-0.16.1.jar=8080:config.yaml
```
- config.yaml
```
---
ssl: false
lowercaseOutputName: false
lowercaseOutputLabelNames: false
rules:
  - pattern: ".*"
```

# sink
{
  "name": "PostgresSinkConnector_0",
  "config": {
    "topics": "nearin.oct_balance",
    "input.data.format": "AVRO",
    "connector.class": "PostgresSink",
    "name": "PostgresSinkConnector_0",
    "kafka.api.key": "G56ECZYIHRWHAVKP",
    "kafka.api.secret": "rfKi+iCeVrS+98vLlznBCepp/TSnZ9RFte4b+usQ5iktU9eV0dsloZPMZB98+Ejq",
    "connection.host": "34.85.112.76",
    "connection.port": "5432",
    "connection.user": "username",
    "connection.password": "password",
    "db.name": "db_mainnet",
    "ssl.mode": "prefer",
    "insert.mode": "UPSERT",
    "db.timezone": "UTC",
    "pk.mode": "record_value",
    "pk.fields": "account",
    "auto.create": "true",
    "auto.evolve": "true",
    "tasks.max": "1"
  }
}
