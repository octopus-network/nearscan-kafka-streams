package network.octopus.nearin;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
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
import static network.octopus.nearin.util.Schemas.Topics.RECEIPTS_OUTCOMES_ACTIONS;
import static network.octopus.nearin.util.Schemas.Topics.TOKEN_DAILY_ACTIVATE_USERS;

public class TokenDailyActiveUsers {
  private static final Logger logger = LoggerFactory.getLogger(TokenDailyActiveUsers.class);
  private static final String DAILY_ACTIVE_USER = "dau";

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
    final String receiptsOutcomesActionsTopic = props.getProperty("receipts_outcomes_actions.topic.name");
    final String tokenDailyActiveUsersTopic = props.getProperty("token_daily_activate_users.topic.name");

    final StreamsBuilder builder = new StreamsBuilder();

    final KStream<String, near.indexer.receipts_outcomes_actions.Value> receiptOutcomeAction = builder
        .stream(receiptsOutcomesActionsTopic,
            Consumed.with(RECEIPTS_OUTCOMES_ACTIONS.keySerde(), RECEIPTS_OUTCOMES_ACTIONS.valueSerde())
                // extrect timestamp in nanoseconds
                .withTimestampExtractor(RECEIPTS_OUTCOMES_ACTIONS.timestampExtractor()));
    receiptOutcomeAction.peek((k, v) -> logger.debug("receipt-outcome-action {} --> {} {}", k, v.getOutcome().getStatus(), v.getAction().getActionKind()));
    
    // filter -> groupby -> windowed -> aggregate -> tostream -> map(key)
    // final KStream<Long, Long> dailyActiveUsers = receiptOutcomeAction
    receiptOutcomeAction
        .filter((key, value) -> value.getReceipt().getReceiverAccountId().equals(tokenAddress)
            && !value.getOutcome().getStatus().equals("FAILURE")
            && value.getAction().getActionKind().equals("FUNCTION_CALL"))
        .map((key, value) -> {
          // new                value.getReceipt().getPredecessorAccountId()
          // mint               argsJson.get("account_id").getAsString()
          // withdraw           value.getReceipt().getPredecessorAccountId()
          // ft_transfer        value.getReceipt().getPredecessorAccountId()
          // ft_transfer_call   value.getReceipt().getPredecessorAccountId()
          final JsonObject jsonObject = new Gson().fromJson(value.getAction().getArgs(), JsonObject.class);
          final String methodName = jsonObject.get("method_name").getAsString();
          if (methodName.equals("mint")) {
            final JsonObject argsJson = jsonObject.get("args_json").getAsJsonObject();
            return KeyValue.pair(DAILY_ACTIVE_USER, argsJson.get("account_id").getAsString());
          } else if (List.of("withdraw", "ft_transfer", "ft_transfer_call").contains(methodName)) {
            return KeyValue.pair(DAILY_ACTIVE_USER, value.getReceipt().getPredecessorAccountId());
          } else {
            return KeyValue.pair(DAILY_ACTIVE_USER, "");
          }
        })
        .groupByKey()
        .windowedBy(TimeWindows.ofSizeAndGrace(Duration.ofDays(1), Duration.ofMinutes(10)))
        // .windowedBy(new DailyTimeWindows(ZoneOffset.UTC, 0, Duration.ofMinutes(10L)))
        .aggregate(HashSet::new, (aggKey, newValue, aggValue) -> {
          if (!newValue.equals("")) {
            aggValue.add(newValue);
          }
          return aggValue;
        })
        .suppress(Suppressed.untilWindowCloses(Suppressed.BufferConfig.unbounded()))
        // .mapValues(value -> value.size());
        .toStream()
        .map((wk, value) -> KeyValue.pair(wk.key(), new near.indexer.daily_activate_users.Value(wk.window().start(), value.size())))
        .to(tokenDailyActiveUsersTopic,
            Produced.with(TOKEN_DAILY_ACTIVATE_USERS.keySerde(), TOKEN_DAILY_ACTIVATE_USERS.valueSerde()));

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
