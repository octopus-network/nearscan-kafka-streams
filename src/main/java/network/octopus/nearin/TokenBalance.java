package network.octopus.nearin;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import network.octopus.nearin.util.Schemas;
import static network.octopus.nearin.util.Schemas.Topics.TOKEN_TRANSFER;
import static network.octopus.nearin.util.Schemas.Topics.TOKEN_BALANCE;

public class TokenBalance {
  private static final Logger logger = LoggerFactory.getLogger(TokenBalance.class);

  public static void main(final String[] args) throws Exception {
    final Properties props = loadConfig(args[0]);
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, props.getProperty("token.name") + "-balance");
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
    final String tokenBalanceTopic = props.getProperty("token_balance.topic.name");

    final StreamsBuilder builder = new StreamsBuilder();

    final KStream<String, near.indexer.token_transfer.Value> tokenTransfer = builder.stream(tokenTransferTopic,
        Consumed.with(TOKEN_TRANSFER.keySerde(), TOKEN_TRANSFER.valueSerde()));

    final KTable<String, near.indexer.token_balance.Value> tokenBalance = tokenTransfer
        .groupByKey()
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
            }, Materialized.with(TOKEN_BALANCE.keySerde(), TOKEN_BALANCE.valueSerde()));

    tokenBalance.toStream()
        .peek((k, v) -> logger.info("token balance: {} --> {} ", k, v))
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

}
