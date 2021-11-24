package network.octopus.nearin;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.WindowStore;
import org.apache.kafka.streams.state.WindowStoreIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import com.google.gson.Gson;
import com.google.gson.JsonObject;

import network.octopus.nearin.util.Schemas;
import static network.octopus.nearin.util.Schemas.Topics.RECEIPTS;
import static network.octopus.nearin.util.Schemas.Topics.EXECUTION_OUTCOMES;
import static network.octopus.nearin.util.Schemas.Topics.ACTION_RECEIPT_ACTIONS;
import static network.octopus.nearin.util.Schemas.Topics.TOKEN_BALANCE;

public class TokenBalance {

  private static final Logger logger = LoggerFactory.getLogger(TokenBalance.class);
  private static final String storeName = "debezium-store";

  public static void main(final String[] args) throws Exception {
    final Properties props = loadConfig(args[0]);
    props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
    props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, SpecificAvroSerde.class);
 
    Schemas.configureSerdes(props);

    final KafkaStreams streams = buildKafkaStreams(props);

    Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

    // final boolean doReset = args.length > 1 && args[1].equals("--reset");
    // if (doReset) {
      streams.cleanUp();
    // }

    startKafkaStreams(streams);
  }

  static KafkaStreams buildKafkaStreams(final Properties props) {
    final String receiptTopic = props.getProperty("receipts.topic.name");
    final String executionOutcomesTopic = props.getProperty("execution_outcomes.topic.name");
    final String actionReceiptActionsTopic = props.getProperty("action_receipt_actions.topic.name");
    final String tokenBalanceTopic = props.getProperty("token_balance.topic.name");
    
    final Duration windowSize = Duration.ofMinutes(10);
    final Duration retentionPeriod = Duration.ofDays(30);

    final StreamsBuilder builder = new StreamsBuilder();

    final StoreBuilder<WindowStore<String, Long>> dedupStoreBuilder = Stores.windowStoreBuilder(
        Stores.inMemoryWindowStore(storeName, retentionPeriod, windowSize, false), Serdes.String(), Serdes.Long());
        // Stores.persistentWindowStore(storeName, retentionPeriod, windowSize, false), Serdes.Long(), Serdes.Long());
    builder.addStateStore(dedupStoreBuilder);

    final KStream<String, abuda.indexer.receipts.Value> receipts = builder
        .stream(receiptTopic,
            Consumed.with(RECEIPTS.keySerde(), RECEIPTS.valueSerde()))
                // .withTimestampExtractor(RECEIPTS.timestampExtractor())) // extrect timestamp in nanoseconds
        .transform(() -> new DeduplicationTransformer<>(windowSize.toMillis(),
            (key, value) -> "receipts-"+value.getReceiptId()), storeName);
    // receipts..peek((k, v) -> logger.info("receipts: {} {}", k, v));
    // receipts.print(Printed.toSysOut());

    final KStream<String, abuda.indexer.execution_outcomes.Value> outcomes = builder
        .stream(executionOutcomesTopic,
            Consumed.with(EXECUTION_OUTCOMES.keySerde(), EXECUTION_OUTCOMES.valueSerde()))
                // .withTimestampExtractor(EXECUTION_OUTCOMES.timestampExtractor())) // extrect timestamp in nanoseconds
        .transform(() -> new DeduplicationTransformer<>(windowSize.toMillis(),
            (key, value) -> "execution_outcomes-"+value.getReceiptId()), storeName);
    // outcomes.print(Printed.toSysOut());
    // outcomes.peek((k, v) -> logger.info("outcomes: {} {}", k, v));

    final KStream<String, abuda.indexer.action_receipt_actions.Value> actions = builder
        .stream(actionReceiptActionsTopic,
            Consumed.with(ACTION_RECEIPT_ACTIONS.keySerde(), ACTION_RECEIPT_ACTIONS.valueSerde()))
                // .withTimestampExtractor(ACTION_RECEIPT_ACTIONS.timestampExtractor())) // extrect timestamp in nanoseconds
        .transform(() -> new DeduplicationTransformer<>(windowSize.toMillis(),
            (key, value) -> "action_receipt_actions-"+value.getReceiptId()+"_"+value.getIndexInActionReceipt()), storeName);
    // actions.print(Printed.toSysOut());
    // actions.peek((k, v) -> logger.info("actions: {} {}", k, v));

    final KStream<String, abuda.indexer.receipts_outcomes_actions.Value> receiptOutcomeAction = receipts
        .join(outcomes, (receipt, outcome) -> new abuda.indexer.receipts_outcomes_actions.Value(receipt, outcome, null),
            JoinWindows.of(Duration.ofMillis(2000)))
        .join(actions,
            (receiptOutcome, action) -> new abuda.indexer.receipts_outcomes_actions.Value(receiptOutcome.getReceipt(),
                receiptOutcome.getOutcome(), action),
            JoinWindows.of(Duration.ofMillis(2000)));
    receiptOutcomeAction.peek((k, v) -> logger.info("[1] joint {} --> {} {}", k, v.getOutcome().getStatus(), v.getAction().getActionKind()));
      // .filter((key, value) -> !value.getOutcome().getStatus().equals("FAILURE") && value.getAction().getActionKind().equals("FUNCTION_CALL"))
      // .peek((k, v) -> logger.info("[2] filter {} --> {}", k, v));

    //
    final Function<abuda.indexer.receipts_outcomes_actions.Value, abuda.indexer.token_transfer.Value.Builder> valueBuilder = (v) -> {
      return abuda.indexer.token_transfer.Value.newBuilder()
        .setReceiptId(v.getReceipt().getReceiptId())
        .setIncludedInBlockHash(v.getReceipt().getIncludedInBlockHash())
        .setIncludedInChunkHash(v.getReceipt().getIncludedInChunkHash())
        .setIndexInChunk(v.getReceipt().getIndexInChunk())
        .setIncludedInBlockTimestamp(v.getReceipt().getIncludedInBlockTimestamp())
        .setPredecessorAccountId(v.getReceipt().getPredecessorAccountId())
        .setReceiverAccountId(v.getReceipt().getReceiverAccountId())
        .setOriginatedFromTransactionHash(v.getReceipt().getOriginatedFromTransactionHash())
        .setGasBurnt(v.getOutcome().getGasBurnt())
        .setTokensBurnt(v.getOutcome().getTokensBurnt())
        .setExecutorAccountId(v.getOutcome().getExecutorAccountId())
        .setStatus(v.getOutcome().getStatus())
        .setShardId(v.getOutcome().getShardId())
        .setIndexInActionReceipt(v.getAction().getIndexInActionReceipt())
        .setActionKind(v.getAction().getActionKind())
        .setArgs(v.getAction().getArgs());
    };

    final KStream<String, abuda.indexer.token_transfer.Value> tokenTransfer = receiptOutcomeAction
    .filter((key, value) -> !value.getOutcome().getStatus().equals("FAILURE") && value.getAction().getActionKind().equals("FUNCTION_CALL"))
    .flatMapValues(value -> {
      final JsonObject jsonObject = new Gson().fromJson(value.getAction().getArgs(), JsonObject.class);
      final String methodName = jsonObject.get("method_name").getAsString();
      final JsonObject argsJson = jsonObject.get("args_json").getAsJsonObject();
      final List<abuda.indexer.token_transfer.Value> result = new ArrayList<>();
      switch (methodName) {
        case "mint": {
          result.add(valueBuilder.apply(value)
              .setAffectedAccount(argsJson.get("account_id").getAsString())
              .setAffectedAmount(new BigDecimal(argsJson.get("amount").getAsString()))
              .setAffectedReason("mint")
              .setTransferFrom(value.getReceipt().getReceiverAccountId())
              .setTransferTo(argsJson.get("account_id").getAsString())
              .build());
          break;
        }
        case "withdraw": {
          result.add(valueBuilder.apply(value)
              .setAffectedAccount(value.getReceipt().getPredecessorAccountId())
              .setAffectedAmount((new BigDecimal(argsJson.get("amount").getAsString())).negate())
              .setAffectedReason("withdraw")
              .setTransferFrom(value.getReceipt().getPredecessorAccountId())
              .setTransferTo(argsJson.get("recipient").getAsString())
              .build());
          break;
        }
        case "ft_transfer": {
          result.add(valueBuilder.apply(value)
              .setAffectedAccount(value.getReceipt().getPredecessorAccountId())
              .setAffectedAmount((new BigDecimal(argsJson.get("amount").getAsString())).negate())
              .setAffectedReason("ft_transfer_from")
              .setTransferFrom(value.getReceipt().getPredecessorAccountId())
              .setTransferTo(argsJson.get("receiver_id").getAsString())
              .build());
          result.add(valueBuilder.apply(value)
              .setAffectedAccount(argsJson.get("receiver_id").getAsString())
              .setAffectedAmount(new BigDecimal(argsJson.get("amount").getAsString()))
              .setAffectedReason("ft_transfer_to")
              .setTransferFrom(value.getReceipt().getPredecessorAccountId())
              .setTransferTo(argsJson.get("receiver_id").getAsString())
              .build());
          break;
        }
        // {
        //   "gas": 5000000000000,
        //   "deposit": "0",
        //   "args_json": {
        //     "amount": "2500000000000000000000000",
        //     "sender_id": "louisliu.near",
        //     "receiver_id": "skyward.near"
        //   },
        //   "args_base64": "eyJzZW5kZXJfaWQiOiJsb3Vpc2xpdS5uZWFyIiwicmVjZWl2ZXJfaWQiOiJza3l3YXJkLm5lYXIiLCJhbW91bnQiOiIyNTAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwIn0=",
        //   "method_name": "ft_resolve_transfer"
        // }
        case "ft_transfer_call": {
          result.add(valueBuilder.apply(value)
              .setAffectedAccount(value.getReceipt().getPredecessorAccountId())
              .setAffectedAmount((new BigDecimal(argsJson.get("amount").getAsString())).negate())
              .setAffectedReason("ft_transfer_call_from")
              .setTransferFrom(value.getReceipt().getPredecessorAccountId())
              .setTransferTo(argsJson.get("receiver_id").getAsString())
              .build());
          result.add(valueBuilder.apply(value)
              .setAffectedAccount(argsJson.get("receiver_id").getAsString())
              .setAffectedAmount(new BigDecimal(argsJson.get("amount").getAsString()))
              .setAffectedReason("ft_transfer_call_to")
              .setTransferFrom(value.getReceipt().getPredecessorAccountId())
              .setTransferTo(argsJson.get("receiver_id").getAsString())
              .build());
          break;
        }
      }
      return result;
    });
    tokenTransfer.peek((k, v) -> logger.info("[2] transfer: {} --> {} [{}] {}", v.getTransferFrom(), v.getTransferTo(), v.getAffectedReason(), v.getAffectedAmount()));

    //
    final KTable<String, abuda.indexer.token_balance.Value> tokenBalance = tokenTransfer
        .groupBy((key, value) -> value.getAffectedAccount())
        .aggregate(abuda.indexer.token_balance.Value::new,
            (aggKey, newValue, aggValue) -> {
              final String account = newValue.getAffectedAccount();
              final BigDecimal amount = newValue.getAffectedAmount();
              final long blockTimestamp = newValue.getIncludedInBlockTimestamp()
                  .divide(new BigDecimal(1e6), RoundingMode.HALF_UP).longValue();
              final String blockHash = newValue.getIncludedInBlockHash();
              final String transactionHash = newValue.getOriginatedFromTransactionHash();
              final String receiptId = newValue.getReceiptId();

              final BigDecimal balance = aggValue.getBalance() == null ? amount : aggValue.getBalance().add(amount);
              final abuda.indexer.token_balance.Value.Builder vb = abuda.indexer.token_balance.Value.newBuilder()
                  .setAccount(account).setBalance(balance);
              if (aggValue.getBlockTimestamp() < blockTimestamp) {
                return vb.setBlockTimestamp(blockTimestamp).setBlockHash(blockHash).setTransactionHash(transactionHash)
                    .setReceiptId(receiptId).build();
              } else {
                return vb.build();
              }
            }, Materialized.with(TOKEN_BALANCE.keySerde(), TOKEN_BALANCE.valueSerde()));

    tokenBalance.toStream()
        .peek((k, v) -> logger.info("[3] balance: {} --> {} ", k, v))
        .to(tokenBalanceTopic, Produced.with(TOKEN_BALANCE.keySerde(), TOKEN_BALANCE.valueSerde()));

    return new KafkaStreams(builder.build(), props);
  }

  static void startKafkaStreams(final KafkaStreams streams) {
    final CountDownLatch latch = new CountDownLatch(1);
    streams.setStateListener((newState, oldState) -> {
      if (newState == KafkaStreams.State.RUNNING && oldState != KafkaStreams.State.RUNNING) {
        latch.countDown();
      }
    });
    
    streams.start();

    try {
      if (!latch.await(60, TimeUnit.SECONDS)) {
        throw new RuntimeException("Streams never finished rebalancing on startup");
      }
    } catch (final InterruptedException e) {
       Thread.currentThread().interrupt();
    }
    // logger.info("Started Service " + SERVICE_APP_ID);
  }

  public static Properties loadConfig(final String configFile) throws IOException {
    if (!Files.exists(Paths.get(configFile))) {
      throw new IOException(configFile + " not found.");
    }
    final Properties props = new Properties();
    try (InputStream inputStream = new FileInputStream(configFile)) {
      props.load(inputStream);
    }
    return props;
  }

  // !!! debezium at least once
  private static class DeduplicationTransformer<K, V, E> implements Transformer<K, V, KeyValue<K, V>> {

    private ProcessorContext context;
    private WindowStore<E, Long> eventIdStore;
    private final long leftDurationMs;
    private final long rightDurationMs;
    private final KeyValueMapper<K, V, E> idExtractor;

    DeduplicationTransformer(final long maintainDurationMs, final KeyValueMapper<K, V, E> idExtractor) {
      if (maintainDurationMs < 1) {
        throw new IllegalArgumentException("maintain duration per event must be >= 1");
      }
      leftDurationMs = maintainDurationMs / 2;
      rightDurationMs = maintainDurationMs - leftDurationMs;
      this.idExtractor = idExtractor;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void init(final ProcessorContext context) {
      this.context = context;
      eventIdStore = (WindowStore<E, Long>) context.getStateStore(storeName);
    }

    public KeyValue<K, V> transform(final K key, final V value) {
      final E eventId = idExtractor.apply(key, value);
      if (isDuplicate(eventId)) {
        logger.info("==================== isDuplicate: {}", eventId);
        updateTimestampOfExistingEventToPreventExpiry(eventId, context.timestamp());
        return null;
      } else {
        rememberNewEvent(eventId, context.timestamp());
        return KeyValue.pair(key, value);
      }
    }

    private boolean isDuplicate(final E eventId) {
      final long eventTime = context.timestamp();
      // logger.info("-------------------- eventTime: {}  eventId: {}", eventTime, eventId);
      final WindowStoreIterator<Long> timeIterator = eventIdStore.fetch(
        eventId,
        eventTime - leftDurationMs,
        eventTime + rightDurationMs);
      final boolean isDuplicate = timeIterator.hasNext();
      timeIterator.close();
      return isDuplicate;
    }

    private void updateTimestampOfExistingEventToPreventExpiry(final E eventId, final long newTimestamp) {
      eventIdStore.put(eventId, newTimestamp, newTimestamp);
    }

    private void rememberNewEvent(final E eventId, final long timestamp) {
      eventIdStore.put(eventId, timestamp, timestamp);
    }

    @Override
    public void close() {
    }
  }

}
