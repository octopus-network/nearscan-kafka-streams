package network.octopus.nearin;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
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
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
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
import org.apache.kafka.streams.kstream.Repartitioned;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.WindowStore;
import org.apache.kafka.streams.state.WindowStoreIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import network.octopus.nearin.util.Schemas;
import static network.octopus.nearin.util.Schemas.Topics.RECEIPTS;
import static network.octopus.nearin.util.Schemas.Topics.EXECUTION_OUTCOMES;
import static network.octopus.nearin.util.Schemas.Topics.ACTION_RECEIPT_ACTIONS;
import static network.octopus.nearin.util.Schemas.Topics.TOKEN_BALANCE;
import static network.octopus.nearin.util.Schemas.Topics.TOKEN_TRANSFER;

public class TokenBalance {

  private static final Logger logger = LoggerFactory.getLogger(TokenBalance.class);
  private static final String storeName = "debezium-store";

  public static void main(final String[] args) throws Exception {
    final Properties props = loadConfig(args[0]);
    props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
    props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, SpecificAvroSerde.class);
    // props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
 
    Schemas.configureSerdes(props);

    final KafkaStreams streams = buildKafkaStreams(props);

    Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

    final boolean doReset = args.length > 1 && args[1].equals("--reset");
    if (doReset) {
      streams.cleanUp();
    }

    startKafkaStreams(streams);
  }

  static KafkaStreams buildKafkaStreams(final Properties props) {
    final String tokenAddress = props.getProperty("token.address");
    final String receiptTopic = props.getProperty("receipts.topic.name");
    final String executionOutcomesTopic = props.getProperty("execution_outcomes.topic.name");
    final String actionReceiptActionsTopic = props.getProperty("action_receipt_actions.topic.name");
    final String tokenTransferTopic = props.getProperty("token_transfer.topic.name");
    final String tokenBalanceTopic = props.getProperty("token_balance.topic.name");
    
    final Duration windowSize = Duration.ofMinutes(60);
    final Duration retentionPeriod = Duration.ofDays(3);

    final StreamsBuilder builder = new StreamsBuilder();

    final StoreBuilder<WindowStore<String, Long>> dedupStoreBuilder = Stores.windowStoreBuilder(
        // Stores.inMemoryWindowStore(storeName, retentionPeriod, windowSize, false), Serdes.String(), Serdes.Long());
        Stores.persistentWindowStore(storeName, retentionPeriod, windowSize, false), Serdes.String(), Serdes.Long());
    builder.addStateStore(dedupStoreBuilder);

    final KStream<String, near.indexer.receipts.Value> receipts = builder
        .stream(receiptTopic,
            Consumed.with(RECEIPTS.keySerde(), RECEIPTS.valueSerde()))
                // .withTimestampExtractor(RECEIPTS.timestampExtractor())) // extrect timestamp in nanoseconds
        .transform(() -> new DeduplicationTransformer<>(windowSize.toMillis(),
            (key, value) -> "receipts-"+value.getReceiptId()), storeName);
    // receipts.peek((k, v) -> logger.debug("receipts: {} {}", k, v));

    final KStream<String, near.indexer.execution_outcomes.Value> outcomes = builder
        .stream(executionOutcomesTopic,
            Consumed.with(EXECUTION_OUTCOMES.keySerde(), EXECUTION_OUTCOMES.valueSerde()))
                // .withTimestampExtractor(EXECUTION_OUTCOMES.timestampExtractor())) // extrect timestamp in nanoseconds
        .transform(() -> new DeduplicationTransformer<>(windowSize.toMillis(),
            (key, value) -> "execution_outcomes-"+value.getReceiptId()), storeName);
    // outcomes.peek((k, v) -> logger.debug("outcomes: {} {}", k, v));

    final KStream<String, near.indexer.action_receipt_actions.Value> actions = builder
        .stream(actionReceiptActionsTopic,
            Consumed.with(ACTION_RECEIPT_ACTIONS.keySerde(), ACTION_RECEIPT_ACTIONS.valueSerde()))
                // .withTimestampExtractor(ACTION_RECEIPT_ACTIONS.timestampExtractor())) // extrect timestamp in nanoseconds
        .transform(() -> new DeduplicationTransformer<>(windowSize.toMillis(),
            (key, value) -> "action_receipt_actions-"+value.getReceiptId()+"_"+value.getIndexInActionReceipt()), storeName);
    // actions.peek((k, v) -> logger.debug("actions: {} {}", k, v));

    final KStream<String, near.indexer.receipts_outcomes_actions.Value> receiptOutcomeAction = receipts
        .join(outcomes, (receipt, outcome) -> new near.indexer.receipts_outcomes_actions.Value(receipt, outcome, null),
            JoinWindows.of(Duration.ofMillis(2000)))
        .join(actions,
            (receiptOutcome, action) -> new near.indexer.receipts_outcomes_actions.Value(receiptOutcome.getReceipt(),
                receiptOutcome.getOutcome(), action),
            JoinWindows.of(Duration.ofMillis(2000)));
    receiptOutcomeAction.peek((k, v) -> logger.debug("receipt-outcome-action {} --> {} {}", k, v.getOutcome().getStatus(), v.getAction().getActionKind()));
      // .filter((key, value) -> !value.getOutcome().getStatus().equals("FAILURE") && value.getAction().getActionKind().equals("FUNCTION_CALL"))
      // .peek((k, v) -> logger.debug("[2] filter {} --> {}", k, v));

    //
    final Function<near.indexer.receipts_outcomes_actions.Value, near.indexer.token_transfer.Value.Builder> valueBuilder = (v) -> {
      return near.indexer.token_transfer.Value.newBuilder()
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

    final KStream<String, near.indexer.token_transfer.Value> tokenTransfer = receiptOutcomeAction
        .filter((key, value) -> value.getReceipt().getReceiverAccountId().equals(tokenAddress)
            && !value.getOutcome().getStatus().equals("FAILURE")
            && value.getAction().getActionKind().equals("FUNCTION_CALL"))
        .flatMapValues(value -> {
          final JsonObject jsonObject = new Gson().fromJson(value.getAction().getArgs(), JsonObject.class);
          final String methodName = jsonObject.get("method_name").getAsString();
          final JsonObject argsJson = jsonObject.get("args_json").getAsJsonObject();
          final List<near.indexer.token_transfer.Value> result = new ArrayList<>();
          switch (methodName) {
              // {
              //   "gas": 100000000000000,
              //   "deposit": "0",
              //   "args_json": {
              //     "metadata": {
              //       "icon": "https://oct.network/assets/img/octfavicon.ico",
              //       "name": "OCTToken",
              //       "spec": "ft-1.0.0",
              //       "symbol": "OCT",
              //       "decimals": 24
              //     },
              //     "owner_id": "madtest.testnet",
              //     "total_supply": "100000000000000000000000000000000"
              //   },
              //   "args_base64": "eyJvd25lcl9pZCI6Im1hZHRlc3QudGVzdG5ldCIsInRvdGFsX3N1cHBseSI6IjEwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMCIsIm1ldGFkYXRhIjp7InNwZWMiOiJmdC0xLjAuMCIsIm5hbWUiOiJPQ1RUb2tlbiIsInN5bWJvbCI6Ik9DVCIsImljb24iOiJodHRwczovL29jdC5uZXR3b3JrL2Fzc2V0cy9pbWcvb2N0ZmF2aWNvbi5pY28iLCJkZWNpbWFscyI6MjR9fQ==",
              //   "method_name": "new"
              // }
              case "new": {
                result.add(valueBuilder.apply(value)
                    .setAffectedAccount(argsJson.get("owner_id").getAsString())
                    .setAffectedAmount(new BigDecimal(argsJson.get("total_supply").getAsString()))
                    .setAffectedReason("new")
                    .setTransferFrom(value.getReceipt().getPredecessorAccountId()) // caller
                    .setTransferTo(argsJson.get("owner_id").getAsString())
                    .build());
                break;
              }
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
              // case "ft_transfer_call": {
              //   result.add(valueBuilder.apply(value)
              //       .setAffectedAccount(value.getReceipt().getPredecessorAccountId())
              //       .setAffectedAmount((new BigDecimal(argsJson.get("amount").getAsString())).negate())
              //       .setAffectedReason("ft_transfer_call_from")
              //       .setTransferFrom(value.getReceipt().getPredecessorAccountId())
              //       .setTransferTo(argsJson.get("receiver_id").getAsString())
              //       .build());
              //   result.add(valueBuilder.apply(value)
              //       .setAffectedAccount(argsJson.get("receiver_id").getAsString())
              //       .setAffectedAmount(new BigDecimal(argsJson.get("amount").getAsString()))
              //       .setAffectedReason("ft_transfer_call_to")
              //       .setTransferFrom(value.getReceipt().getPredecessorAccountId())
              //       .setTransferTo(argsJson.get("receiver_id").getAsString())
              //       .build());
              //   break;
              // }

              // {
              // "gas": 5000000000000,
              // "deposit": "0",
              // "args_json": {
              // "amount": "2500000000000000000000000",
              // "sender_id": "louisliu.near",
              // "receiver_id": "skyward.near"
              // },
              // "args_base64": "eyJzZW5kZXJfaWQiOiJsb3Vpc2xpdS5uZWFyIiwicmVjZWl2ZXJfaWQiOiJza3l3YXJkLm5lYXIiLCJhbW91bnQiOiIyNTAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwIn0=",
              // "method_name": "ft_resolve_transfer"
              // }
              case "ft_resolve_transfer": {
                result.add(valueBuilder.apply(value)
                    .setAffectedAccount(argsJson.get("sender_id").getAsString())
                    .setAffectedAmount((new BigDecimal(argsJson.get("amount").getAsString())).negate())
                    .setAffectedReason("ft_resolve_transfer_from")
                    .setTransferFrom(argsJson.get("sender_id").getAsString())
                    .setTransferTo(argsJson.get("receiver_id").getAsString())
                    .build());
                result.add(valueBuilder.apply(value)
                    .setAffectedAccount(argsJson.get("receiver_id").getAsString())
                    .setAffectedAmount(new BigDecimal(argsJson.get("amount").getAsString()))
                    .setAffectedReason("ft_resolve_transfer_to")
                    .setTransferFrom(argsJson.get("sender_id").getAsString())
                    .setTransferTo(argsJson.get("receiver_id").getAsString())
                    .build());
                break;
              }
          }
          return result;
        });
    tokenTransfer.peek((k, v) -> logger.info("transfer: {} --> {} [{}] {}", v.getTransferFrom(), v.getTransferTo(), v.getAffectedReason(), v.getAffectedAmount()));
    
    // token transfer
    tokenTransfer
        .repartition(Repartitioned.numberOfPartitions(1))
        .to(tokenTransferTopic, Produced.with(TOKEN_TRANSFER.keySerde(), TOKEN_TRANSFER.valueSerde()));

    // token balance
    final KTable<String, near.indexer.token_balance.Value> tokenBalance = tokenTransfer
        .groupBy((key, value) -> value.getAffectedAccount())
        .aggregate(near.indexer.token_balance.Value::new,
            (aggKey, newValue, aggValue) -> {
              final BigDecimal affectedAmount = newValue.getAffectedAmount();
              final BigDecimal includedInBlockTimestamp = newValue.getIncludedInBlockTimestamp();
              final int indexInChunk = newValue.getIndexInChunk();

              final BigDecimal aggBalance = aggValue.getBalance();
              final BigDecimal aggBlockTimestamp = aggValue.getBlockTimestamp();
              final int aggIndexInChunk = aggValue.getIndexInChunk();

              if (aggBalance == null && aggBlockTimestamp == null) {
                aggValue.setAccount(aggKey);
                aggValue.setBalance(affectedAmount);
                aggValue.setBlockTimestamp(includedInBlockTimestamp);
                aggValue.setBlockHash(newValue.getIncludedInBlockHash());
                aggValue.setChunkHash(newValue.getIncludedInChunkHash());
                aggValue.setIndexInChunk(indexInChunk);
                aggValue.setTransactionHash(newValue.getOriginatedFromTransactionHash());
                aggValue.setReceiptId(newValue.getReceiptId());
              } else {
                Boolean update = true;
                if (aggBlockTimestamp.compareTo(includedInBlockTimestamp) > 0) {
                  update = false;
                } else if (aggBlockTimestamp.compareTo(includedInBlockTimestamp) == 0 && aggIndexInChunk > indexInChunk) {
                  update = false;
                }
                if (update) {
                  aggValue.setBlockTimestamp(includedInBlockTimestamp);
                  aggValue.setBlockHash(newValue.getIncludedInBlockHash());
                  aggValue.setChunkHash(newValue.getIncludedInChunkHash());
                  aggValue.setIndexInChunk(indexInChunk);
                  aggValue.setTransactionHash(newValue.getOriginatedFromTransactionHash());
                  aggValue.setReceiptId(newValue.getReceiptId());
                }
                aggValue.setBalance(aggBalance.add(affectedAmount));
              }
              return aggValue;

              // return near.indexer.token_balance.Value.newBuilder()
              //     .setAccount(newValue.getAffectedAccount())
              //     .setBalance(aggBalance == null ? affectedAmount : aggBalance.add(affectedAmount))
              //     .setBlockTimestamp(update ? includedInBlockTimestamp : aggBlockTimestamp)
              //     .setBlockHash(update ? newValue.getIncludedInBlockHash() : aggValue.getBlockHash())
              //     .setChunkHash(update ? newValue.getIncludedInChunkHash() : aggValue.getChunkHash())
              //     .setIndexInChunk(update ? indexInChunk : aggIndexInChunk)
              //     .setTransactionHash(update ? newValue.getOriginatedFromTransactionHash() : aggValue.getTransactionHash())
              //     .setReceiptId(update ? newValue.getReceiptId() : aggValue.getReceiptId())
              //     .build();
            }, Materialized.with(TOKEN_BALANCE.keySerde(), TOKEN_BALANCE.valueSerde()));

    tokenBalance.toStream()
        .peek((k, v) -> logger.debug("[3] balance: {} --> {} ", k, v))
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
        logger.info("[EE]isDuplicate: {}", eventId);
        updateTimestampOfExistingEventToPreventExpiry(eventId, context.timestamp());
        return null;
      } else {
        rememberNewEvent(eventId, context.timestamp());
        return KeyValue.pair(key, value);
      }
    }

    private boolean isDuplicate(final E eventId) {
      final long eventTime = context.timestamp();
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
