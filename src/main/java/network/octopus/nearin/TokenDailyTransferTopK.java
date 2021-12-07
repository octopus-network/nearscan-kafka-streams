package network.octopus.nearin;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
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
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.Suppressed;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Windowed;
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
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, props.getProperty("token.name") + "-daily-transfer-topk");
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

    final KStream<Windowed<String>, List<near.indexer.token_transfer.Value>> dailyTransferTopk = tokenTransfer
        .filter((key, value) -> TRANSFER_ACTIONS.contains(value.getAffectedReason()))
        .map((key, value) -> KeyValue.pair(DAILY_TRANSFER_TOPK, value))
        .groupByKey()
        .windowedBy(TimeWindows.ofSizeAndGrace(Duration.ofDays(1), Duration.ofMinutes(10)))
        .aggregate(ArrayList<near.indexer.token_transfer.Value>::new, (aggKey, newValue, aggValue) -> {
          aggValue.add(newValue);
          // amount desc -> account asc -> timestamp asc
          aggValue.sort((e1, e2) -> {
            final int c1 = e2.getAffectedAmount().abs().compareTo(e1.getAffectedAmount().abs());
            if (c1 != 0) {
              return c1;
            }
            final int c2 = e1.getAffectedAccount().compareTo(e2.getAffectedAccount());
            if (c2 != 0) {
              return c2;
            }
            final int c3 = e1.getIncludedInBlockTimestamp().compareTo(e2.getIncludedInBlockTimestamp());
            return c3;
          });
          if (aggValue.size() > DAILY_TRANSFER_TOPK_COUNT) {
            aggValue.remove(aggValue.size() - 1);
          }
          return aggValue;
        }, Materialized.with(Serdes.String(), Serdes.ListSerde(ArrayList.class, TOKEN_TRANSFER.valueSerde())))
        .suppress(Suppressed.untilTimeLimit(Duration.ofMinutes(10), Suppressed.BufferConfig.unbounded()))
        .toStream();

    dailyTransferTopk
        // .peek((key, value) -> logger.debug("{} : {}", key, value))
        .flatMap((wk, value) -> {
          final List<KeyValue<String, near.indexer.daily_transfer_topk.Value>> result = new ArrayList<>();
          int index = 0;
          for (final near.indexer.token_transfer.Value item : value) {
            result.add(KeyValue.pair(wk.key(), new near.indexer.daily_transfer_topk.Value(wk.window().end(), index, item)));
            index += 1;
          }
          return result;
        })
        .peek((key, value) -> logger.info("Daily transfer topk: {} - {}", key, value))
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

}
