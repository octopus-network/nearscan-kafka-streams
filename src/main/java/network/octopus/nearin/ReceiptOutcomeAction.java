package network.octopus.nearin;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Produced;
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
import static network.octopus.nearin.util.Schemas.Topics.RECEIPTS_OUTCOMES_ACTIONS;

public class ReceiptOutcomeAction {
  
  private static final Logger logger = LoggerFactory.getLogger(ReceiptOutcomeAction.class);
  private static final String storeName = "nearin-debezium-store";

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
    final String receiptTopic = props.getProperty("receipts.topic.name");
    final String executionOutcomesTopic = props.getProperty("execution_outcomes.topic.name");
    final String actionReceiptActionsTopic = props.getProperty("action_receipt_actions.topic.name");
    final String receiptsOutcomesActionsTopic = props.getProperty("receipts_outcomes_actions.topic.name");
    
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
        .transform(() -> new DeduplicationTransformer<>(windowSize.toMillis(),
            (key, value) -> "receipts-"+value.getReceiptId()), storeName);
    // receipts.peek((k, v) -> logger.debug("receipts: {} {}", k, v));

    final KStream<String, near.indexer.execution_outcomes.Value> outcomes = builder
        .stream(executionOutcomesTopic,
            Consumed.with(EXECUTION_OUTCOMES.keySerde(), EXECUTION_OUTCOMES.valueSerde()))
        .transform(() -> new DeduplicationTransformer<>(windowSize.toMillis(),
            (key, value) -> "execution_outcomes-"+value.getReceiptId()), storeName);
    // outcomes.peek((k, v) -> logger.debug("outcomes: {} {}", k, v));

    final KStream<String, near.indexer.action_receipt_actions.Value> actions = builder
        .stream(actionReceiptActionsTopic,
            Consumed.with(ACTION_RECEIPT_ACTIONS.keySerde(), ACTION_RECEIPT_ACTIONS.valueSerde()))
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
      // .filter((key, value) -> !value.getOutcome().getStatus().equals("FAILURE") && value.getAction().getActionKind().equals("FUNCTION_CALL"))
      // .peek((k, v) -> logger.debug("[2] filter {} --> {}", k, v));

    receiptOutcomeAction
        .peek((k, v) -> logger.debug("receipt-outcome-action {} --> {} {}", k, v.getOutcome().getStatus(), v.getAction().getActionKind()))
        .to(receiptsOutcomesActionsTopic, Produced.with(RECEIPTS_OUTCOMES_ACTIONS.keySerde(), RECEIPTS_OUTCOMES_ACTIONS.valueSerde()));

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
