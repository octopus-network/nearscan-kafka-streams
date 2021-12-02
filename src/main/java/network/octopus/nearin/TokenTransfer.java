package network.octopus.nearin;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Paths;
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
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import network.octopus.nearin.util.Schemas;
import static network.octopus.nearin.util.Schemas.Topics.RECEIPTS_OUTCOMES_ACTIONS;
import static network.octopus.nearin.util.Schemas.Topics.TOKEN_TRANSFER;

public class TokenTransfer {
  private static final Logger logger = LoggerFactory.getLogger(TokenTransfer.class);

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
    final String tokenTransferTopic = props.getProperty("token_transfer.topic.name");

    final StreamsBuilder builder = new StreamsBuilder();

    final KStream<String, near.indexer.receipts_outcomes_actions.Value> receiptOutcomeAction = builder
        .stream(receiptsOutcomesActionsTopic,
            Consumed.with(RECEIPTS_OUTCOMES_ACTIONS.keySerde(), RECEIPTS_OUTCOMES_ACTIONS.valueSerde())
                // extrect timestamp in nanoseconds
                .withTimestampExtractor(RECEIPTS_OUTCOMES_ACTIONS.timestampExtractor()));
    receiptOutcomeAction.peek((k, v) -> logger.debug("receipt-outcome-action {} --> {} {}", k, v.getOutcome().getStatus(), v.getAction().getActionKind()));

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

        tokenTransfer
            .peek((k, v) -> logger.debug("transfer: {} --> {} [{}] {}", v.getTransferFrom(), v.getTransferTo(), v.getAffectedReason(), v.getAffectedAmount()))
            .map((key, value) -> KeyValue.pair(value.getAffectedAccount(), value)) // <account, transfer>
            .to(tokenTransferTopic, Produced.with(TOKEN_TRANSFER.keySerde(), TOKEN_TRANSFER.valueSerde()));

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
