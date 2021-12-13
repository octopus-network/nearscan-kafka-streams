# vm
gcloud beta compute ssh --zone us-central1-a near-indexer-testnet --project nearin
ssh -i ~/.ssh/google_compute_engine deallinker-ry@35.193.214.76


sudo apt install libpq-dev git clang make

```
https://cloud.google.com/compute/docs/disks/add-persistent-disk

sudo lsblk

sudo mkfs.ext4 -m 0 -E lazy_itable_init=0,lazy_journal_init=0,discard /dev/sdb

sudo mkdir -p /mnt/disks/indexer
sudo mount -o discard,defaults /dev/sdb /mnt/disks/indexer
sudo chmod a+w /mnt/disks/indexer

sudo cp /etc/fstab /etc/fstab.backup
sudo blkid /dev/sdb
UUID=ed1d80eb-7c2a-43a4-b9b7-71edd5ff80df /mnt/disks/indexer ext4 discard,defaults,nofail 0 2
```

# sql
```
cloudsql.enable_pglogical=on (https://cloud.google.com/sql/docs/postgres/replication/configure-logical-replication#setting-up-native-postgresql-logical-replication)

postgres : 5cbohgggbIlANfdJ
indexer : jrs5phg39gpW8Xjm
debezium : g5186lNHxH7XPUkK

ALTER USER indexer WITH REPLICATION;
ALTER USER debezium WITH REPLICATION;
```

# indexer
DATABASE_URL="postgres://indexer:jrs5phg39gpW8Xjm@34.69.2.106/db_testnet?options=-c search_path%3Dindexer"

cargo install diesel_cli --no-default-features --features "postgres"
diesel migration run

cargo run --release -- --home-dir /mnt/disks/indexer/.near/testnet init --chain-id testnet --download-genesis
cargo run --release -- --home-dir /mnt/disks/indexer/.near/testnet run --store-genesis --stream-while-syncing --concurrency 1 sync-from-latest
## oct.beta_oct_relay.testnet : 61741054
cargo run --release -- --home-dir /mnt/disks/indexer/.near/testnet run --store-genesis --stream-while-syncing --non-strict-mode --concurrency 1 sync-from-block --height 61730000 2>&1 | tee -a ../indexer.log



select * from action_receipt_actions ara 
  where receipt_receiver_account_id = 'oct-token.testnet'
  order by receipt_included_in_block_timestamp asc
  limit 10;


# kafka
CREATE TABLE IF NOT EXISTS indexer.heartbeat (id SERIAL PRIMARY KEY, ts TIMESTAMP WITH TIME ZONE);
INSERT INTO indexer.heartbeat (id, ts) VALUES (1, NOW()) ON CONFLICT(id) DO UPDATE SET ts=EXCLUDED.ts;

{
  "name": "NearinTestnetCDC_0",
  "config": {
    "connector.class": "PostgresCdcSource",
    "name": "NearinTestnetCDC_0",
    "kafka.api.key": "IBAR2ZPPEFB5PYFQ",
    "kafka.api.secret": "d13VczfldBHfoJIKguEL5fIdfZ/L4/revV7lFaAvFziuvyClQLjyUKoor3RrXvHI",
    "database.hostname": "34.69.2.106",
    "database.port": "5432",
    "database.user": "indexer",
    "database.password": "jrs5phg39gpW8Xjm",
    "database.dbname": "db_testnet",
    "database.server.name": "near",
    "database.sslmode": "disable",
    "table.include.list": "indexer.receipts, indexer.action_receipt_actions, indexer.execution_outcomes, indexer.data_receipts, indexer.action_receipt_input_data, indexer.action_receipt_output_data, indexer.heartbeat",
    "slot.name": "cdc_0",
    "heartbeat.interval.ms": "60000",
    "heartbeat.action.query": "INSERT INTO indexer.heartbeat (id, ts) VALUES (1, NOW()) ON CONFLICT(id) DO UPDATE SET ts=EXCLUDED.ts",
    "output.data.format": "AVRO",
    "tasks.max": "1",
    "transforms": "ExtractReceiptID,ExtractDataID,ExtractInputDataID,ExtractOutputDataID ",
    "predicates": "IsReceipt,IsData,IsInputData,IsOutputData ",
    "transforms.ExtractReceiptID.type": "org.apache.kafka.connect.transforms.ExtractField$Key",
    "transforms.ExtractReceiptID.field": "receipt_id",
    "transforms.ExtractReceiptID.predicate": "IsReceipt",
    "transforms.ExtractDataID.type": "org.apache.kafka.connect.transforms.ExtractField$Key",
    "transforms.ExtractDataID.field": "data_id",
    "transforms.ExtractDataID.predicate": "IsData",
    "transforms.ExtractInputDataID.type": "org.apache.kafka.connect.transforms.ExtractField$Key",
    "transforms.ExtractInputDataID.field": "input_data_id",
    "transforms.ExtractInputDataID.predicate": "IsInputData",
    "transforms.ExtractOutputDataID.type": "org.apache.kafka.connect.transforms.ExtractField$Key",
    "transforms.ExtractOutputDataID.field": "output_data_id",
    "transforms.ExtractOutputDataID.predicate": "IsOutputData",
    "predicates.IsReceipt.type": "org.apache.kafka.connect.transforms.predicates.TopicNameMatches",
    "predicates.IsReceipt.pattern": ".*indexer\\.receipts|.*indexer\\.execution_outcomes|.*indexer\\.action_receipt_actions",
    "predicates.IsData.type": "org.apache.kafka.connect.transforms.predicates.TopicNameMatches",
    "predicates.IsData.pattern": ".*indexer\\.data_receipts",
    "predicates.IsInputData.type": "org.apache.kafka.connect.transforms.predicates.TopicNameMatches",
    "predicates.IsInputData.pattern": ".*indexer\\.action_receipt_input_data",
    "predicates.IsOutputData.type": "org.apache.kafka.connect.transforms.predicates.TopicNameMatches",
    "predicates.IsOutputData.pattern": ".*indexer\\.action_receipt_output_data"
  }
}

## dev 
XWE3HSVOCTPTKZSN
Z3jPle0D19hAKyOCPKzCxx+/9kP9dhypqkQGz/NZaznDcGkFYFQpi0eCrBDuZ5yy


```
transforms=ExtractReceiptID,ExtractDataID,ExtractInputDataID,ExtractOutputDataID

transforms.ExtractReceiptID.type=org.apache.kafka.connect.transforms.ExtractField$Key
transforms.ExtractReceiptID.field=receipt_id
transforms.ExtractReceiptID.predicate=IsReceipt

transforms.ExtractDataID.type=org.apache.kafka.connect.transforms.ExtractField$Key
transforms.ExtractDataID.field=data_id
transforms.ExtractDataID.predicate=IsData

transforms.ExtractInputDataID.type=org.apache.kafka.connect.transforms.ExtractField$Key
transforms.ExtractInputDataID.field=input_data_id
transforms.ExtractInputDataID.predicate=IsInputData

transforms.ExtractOutputDataID.type=org.apache.kafka.connect.transforms.ExtractField$Key
transforms.ExtractOutputDataID.field=output_data_id
transforms.ExtractOutputDataID.predicate=IsOutputData


predicates=IsReceipt,IsData,IsInputData,IsOutputData

predicates.IsReceipt.type=org.apache.kafka.connect.transforms.predicates.TopicNameMatches
predicates.IsReceipt.pattern=.*indexer\.receipts|.*indexer\.execution_outcomes|.*indexer\.action_receipt_actions

predicates.IsData.type=org.apache.kafka.connect.transforms.predicates.TopicNameMatches
predicates.IsData.pattern=.*indexer\.data_receipts

predicates.IsInputData.type=org.apache.kafka.connect.transforms.predicates.TopicNameMatches
predicates.IsInputData.pattern=.*indexer\.action_receipt_input_data

predicates.IsOutputData.type=org.apache.kafka.connect.transforms.predicates.TopicNameMatches
predicates.IsOutputData.pattern=.*indexer\.action_receipt_output_data
```


# aws code
ruanyu-at-425660444042
uS69V9gu/G7+DifW5F8XaGUsSnMqtGA4akQ/JFb2Cj0=


# confluent
create topic      :   nearin.oct_transfer, nearin.oct_balance
register schema   :   mvn schema-registry:register


# build & run
mvn schema-registry:register
mvn compile jib:dockerBuild


docker run -d -p 7071:7071 -p 7072:7072 \
  -v $(pwd)/src/main/resources/config/dev.properties:/nearin/config.properties \
  nearin:0.0.1

docker run -it --rm -p 7071:7071 -p 7072:7072 \
  -v $(pwd)/src/main/resources/config/dev.properties:/nearin/config.properties \
  nearin:0.0.1

docker run -it --rm -p 7071:7071 -p 7072:7072 \
  -v $(pwd)/src/main/resources/config/dev.properties:/nearin/config.properties \
  --entrypoint bash \
  nearin:0.0.1

java -javaagent:/nearin/jmx_prometheus_javaagent-0.16.1.jar=7071:/nearin/streams-config.yml -javaagent:/nearin/jolokia-jvm-1.7.1.jar=port=7072,host=* -Xdebug -cp @/app/jib-classpath-file network.octopus.nearin.TokenTransfer /nearin/config.properties

java -javaagent:/nearin/jmx_prometheus_javaagent-0.16.1.jar=7071:/nearin/streams-config.yml -javaagent:/nearin/jolokia-jvm-1.7.1.jar=port=7072,host=* -Xdebug -cp @/app/jib-classpath-file network.octopus.nearin.TokenBalance /nearin/config.properties


# sink
```
CREATE TABLE nearin.oct_transfer (
  receipt_id text NOT NULL,
  included_in_block_hash text NOT NULL,
  included_in_chunk_hash text NOT NULL,
  index_in_chunk int4 NOT NULL,
  included_in_block_timestamp numeric(20) NOT NULL,
  predecessor_account_id text NOT NULL,
  receiver_account_id text NOT NULL,
  originated_from_transaction_hash text NOT NULL,
  gas_burnt numeric(20) NOT NULL,
  tokens_burnt numeric(45) NOT NULL,
  executor_account_id text NOT NULL,
  status text NOT NULL, -- enum
  shard_id numeric(20) NOT NULL,
  index_in_action_receipt int4 NOT NULL,
  action_kind text NOT NULL, -- enum
  args text NOT NULL, -- jsonb
  affected_account text NOT NULL,
  affected_amount numeric(45) NOT NULL,
  affected_reason text NOT NULL,
  transfer_from text NOT NULL,
  transfer_to text NOT NULL
);
```