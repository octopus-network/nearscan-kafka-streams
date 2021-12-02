package network.octopus.nearin;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TreeSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import network.octopus.nearin.util.Schemas;
import static network.octopus.nearin.util.Schemas.Topics.TOKEN_TRANSFER;
import static network.octopus.nearin.util.Schemas.Topics.TOKEN_DAILY_TRANSFER_TOPK;

public class TokenDailyTransferTopK {
  private static final Logger logger = LoggerFactory.getLogger(TokenDailyTransferTopK.class);
  private static final String DAILY_TRANSFER_TOPK = "topk";
  private static final int DAILY_TRANSFER_TOPK_COUNT = 10;
  private static final List<String> TRANSFER_ACTIONS = List.of("new", "mint", "withdraw", "ft_transfer_from", "ft_transfer_call_from");

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
    final String tokenTransferTopic = props.getProperty("token_transfer.topic.name");
    final String tokenDailyTransferTopKTopic = props.getProperty("token_daily_transfer_topk.topic.name");

    final StreamsBuilder builder = new StreamsBuilder();

    final KStream<String, near.indexer.token_transfer.Value> tokenTransfer = builder
        .stream(tokenTransferTopic,
            Consumed.with(TOKEN_TRANSFER.keySerde(), TOKEN_TRANSFER.valueSerde())
                .withTimestampExtractor(TOKEN_TRANSFER.timestampExtractor())); // extrect timestamp in nanoseconds
    tokenTransfer.peek((k, v) -> logger.debug("transfer: {} --> {} [{}] {}", v.getTransferFrom(), v.getTransferTo(), v.getAffectedReason(), v.getAffectedAmount()));

    tokenTransfer
        .filter((key, value) -> TRANSFER_ACTIONS.contains(value.getAffectedReason()))
        .map((key, value) -> KeyValue.pair(DAILY_TRANSFER_TOPK, value))
        .groupByKey()
        .windowedBy(TimeWindows.ofSizeAndGrace(Duration.ofDays(1), Duration.ofMinutes(10)))
        .aggregate(TopKTransfers::new, (aggKey, newValue, aggValue) -> {
          aggValue.add(newValue);
          return aggValue;
        })
        .suppress(Suppressed.untilTimeLimit(Duration.ofMinutes(10), Suppressed.BufferConfig.unbounded()))
        .toStream()
        .flatMap((wk, value) -> {
          final List<KeyValue<String, near.indexer.daily_transfer_topk.Value>> result = new ArrayList<>();
          int index = 0;
          for (final near.indexer.token_transfer.Value item : value) {
            result.add(KeyValue.pair(wk.key(), new near.indexer.daily_transfer_topk.Value(wk.window().end(), index, item)));
            index += 1;
          }
          return result;
        })
        .to(tokenDailyTransferTopKTopic,
            Produced.with(TOKEN_DAILY_TRANSFER_TOPK.keySerde(), TOKEN_DAILY_TRANSFER_TOPK.valueSerde()));

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

  // top k 10
  static class TopKTransfers implements Iterable<near.indexer.token_transfer.Value> {
    private final Map<String, near.indexer.token_transfer.Value> current = new HashMap<>();
    private final TreeSet<near.indexer.token_transfer.Value> topK = new TreeSet<>((e1, e2) -> {
      final BigDecimal amount1 = e1.getAffectedAmount().abs();
      final BigDecimal amount2 = e2.getAffectedAmount().abs();

      final int result = amount2.compareTo(amount1);
      if (result != 0) {
        return result;
      }
      final String account1 = e1.getAffectedAccount();
      final String account2 = e2.getAffectedAccount();
      return account1.compareTo(account2);
    });

    @Override
    public String toString() {
      return current.toString();
    }

    public void add(final near.indexer.token_transfer.Value transfer) {
      if(current.containsKey(transfer.getAffectedAccount())) {
        topK.remove(current.remove(transfer.getAffectedAccount()));
      }
      topK.add(transfer);
      current.put(transfer.getAffectedAccount(), transfer);
      if (topK.size() > DAILY_TRANSFER_TOPK_COUNT) {
        final near.indexer.token_transfer.Value last = topK.last();
        current.remove(last.getAffectedAccount());
        topK.remove(last);
      }
    }

    void remove(final near.indexer.token_transfer.Value value) {
      topK.remove(value);
      current.remove(value.getAffectedAccount());
    }

    @Override
    public Iterator<near.indexer.token_transfer.Value> iterator() {
      return topK.iterator();
    }
  }
  
}
