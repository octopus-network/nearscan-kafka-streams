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
    "table.include.list": "indexer.receipts, indexer.action_receipt_actions, indexer.execution_outcomes, indexer.heartbeat",
    "slot.name": "cdc_0",
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



# aws code
ruanyu-at-425660444042
uS69V9gu/G7+DifW5F8XaGUsSnMqtGA4akQ/JFb2Cj0=


# confluent
create topic      :   nearin.oct_balance
register schema   :   mvn schema-registry:register


# build & run
mvn schema-registry:register
mvn compile jib:dockerBuild

docker run -d -p 7071:7071 -p 7072:7072 \
  -v $(pwd)/src/main/resources/config/dev.properties:/nearin/config.properties \
  token-balance:0.0.1


docker run -it --rm -p 7071:7071 -p 7072:7072 \
  -v $(pwd)/src/main/resources/config/dev.properties:/nearin/config.properties \
  token-balance:0.0.1