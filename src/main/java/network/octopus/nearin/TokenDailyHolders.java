package network.octopus.nearin;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.Suppressed;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import network.octopus.nearin.util.Schemas;
import static network.octopus.nearin.util.Schemas.Topics.TOKEN_TRANSFER;
import static network.octopus.nearin.util.Schemas.Topics.TOKEN_DAILY_HOLDERS;

public class TokenDailyHolders {
  private static final Logger logger = LoggerFactory.getLogger(TokenDailyHolders.class);
  private static final String storeName = "daily-holders";

  public static void main(final String[] args) throws Exception {
    final Properties props = loadConfig(args[0]);
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, props.getProperty("token.name") + "-daily-holders");
    props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
    props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
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
    final String tokenTransferTopic = props.getProperty("token_transfer.topic.name");
    final String tokenDailyHolderTopic = props.getProperty("token_daily_holders.topic.name");

    final StreamsBuilder builder = new StreamsBuilder();

    final StoreBuilder<KeyValueStore<String, near.indexer.daily_holders.Value>> cumsumStoreBuilder = Stores
        .keyValueStoreBuilder(Stores.persistentKeyValueStore(storeName), 
            Serdes.String(), TOKEN_DAILY_HOLDERS.valueSerde()); // .withLoggingEnabled(new HashMap<>());
    builder.addStateStore(cumsumStoreBuilder);

    final KStream<String, near.indexer.token_transfer.Value> tokenTransfer = builder
    .stream(tokenTransferTopic,
        Consumed.with(TOKEN_TRANSFER.keySerde(), TOKEN_TRANSFER.valueSerde())
            .withTimestampExtractor(TOKEN_TRANSFER.timestampExtractor())); // extrect timestamp in nanoseconds
    tokenTransfer.peek((k, v) -> logger.debug("transfer: {} --> {} [{}] {}", v.getTransferFrom(), v.getTransferTo(), v.getAffectedReason(), v.getAffectedAmount()));

    tokenTransfer
        .groupByKey()
        .windowedBy(TimeWindows.ofSizeAndGrace(Duration.ofDays(1), Duration.ofMinutes(10)))
        .aggregate(() -> "0",
            (aggKey, newValue, aggValue) -> (newValue.getAffectedAmount().add(new BigDecimal(aggValue))).toString())
        .suppress(Suppressed.untilWindowCloses(Suppressed.BufferConfig.unbounded()))
        .toStream()
        .map((wk, value) -> KeyValue.pair(wk.key(),
            new near.indexer.daily_holders.Value(wk.key(), wk.window().end(), new BigDecimal(value))))
        .transform(() -> new CumulativeSumTransformer(), storeName)
        .peek((key, value) -> logger.info("Daily balance: {} - {}", key, value))
        .to(tokenDailyHolderTopic, Produced.with(TOKEN_DAILY_HOLDERS.keySerde(), TOKEN_DAILY_HOLDERS.valueSerde()));

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

  // Cumulative Sum
  private static class CumulativeSumTransformer implements Transformer<String, near.indexer.daily_holders.Value, KeyValue<String, near.indexer.daily_holders.Value>> {
    private KeyValueStore<String, near.indexer.daily_holders.Value> store;

    @Override
    public void init(final ProcessorContext context) {
      store = context.getStateStore(storeName);
    }

    public KeyValue<String, near.indexer.daily_holders.Value> transform(final String key, final near.indexer.daily_holders.Value value) {
      final near.indexer.daily_holders.Value prev = store.get(key);
      if (prev == null) {
        if (value.getAmount().compareTo(new BigDecimal(0L)) < 0) {
          logger.error("[EE] Negative balance: {} - {}", key, value.getAmount());
        }
        store.put(key, value);
        return KeyValue.pair(key, value);
      } else {
        final BigDecimal cumsum = prev.getAmount().add(value.getAmount());
        if (cumsum.compareTo(new BigDecimal(0L)) > 0) {
          final near.indexer.daily_holders.Value post = new near.indexer.daily_holders.Value(value.getAccount(), value.getTimestamp(), cumsum);
          store.put(key, post);
          return KeyValue.pair(key, post);
        } else {
          if (cumsum.compareTo(new BigDecimal(0L)) < 0) {
            logger.error("[EE] Negative balance: {} - {} - {}", key, cumsum, value);
          }
          store.delete(key);
          return null;
        }
      }
    }

    @Override
    public void close() {
    }
  }

}
