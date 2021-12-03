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
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, props.getProperty("token.name") + "-transfer");
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
    receiptOutcomeAction.peek((k, v) -> logger.debug("receipt-outcome-action {} --> {}", k, v));

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
        .flatMap((key, value) -> {
          final String predecessor = value.getReceipt().getPredecessorAccountId();
          final String receiver = value.getReceipt().getReceiverAccountId();
          final JsonObject jsonObject = new Gson().fromJson(value.getAction().getArgs(), JsonObject.class);
          final String methodName = jsonObject.get("method_name").getAsString();
          final JsonObject argsJson = jsonObject.get("args_json").getAsJsonObject();
          final List<KeyValue<String, near.indexer.token_transfer.Value>> result = new ArrayList<>();
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
              //   "method_name": "new"
              // }
              case "new": {
                if (argsJson.has("owner_id") && argsJson.has("total_supply") && argsJson.has("metadata")) {
                  final String owner_id = argsJson.get("owner_id").getAsString();
                  final String total_supply = argsJson.get("total_supply").getAsString();
                  final JsonObject metadata = argsJson.get("metadata").getAsJsonObject();
                  if (metadata.has("spec") && metadata.get("spec").getAsString().contains("ft-")) {
                    result.add(KeyValue.pair(owner_id, 
                        valueBuilder.apply(value)
                            .setAffectedAccount(owner_id)
                            .setAffectedAmount(new BigDecimal(total_supply))
                            .setAffectedReason("new")
                            .setTransferFrom(predecessor) // caller
                            .setTransferTo(owner_id)
                            .build()));
                    // TODO: monitor new ft-1.0.0 contracts
                  }
                }
                break;
              }
              // {
              //   "gas": 10000000000000,
              //   "deposit": "58280000000000000000000",
              //   "args_json": {
              //       "amount": "2500000000000000000000000",
              //       "account_id": "louisliu.near"
              //   },
              //   "method_name": "mint"
              // }
              case "mint": {
                final String account_id = argsJson.get("account_id").getAsString();
                final String amount = argsJson.get("amount").getAsString();
                result.add(KeyValue.pair(account_id, 
                    valueBuilder.apply(value)
                        .setAffectedAccount(account_id)
                        .setAffectedAmount(new BigDecimal(amount))
                        .setAffectedReason("mint")
                        .setTransferFrom(receiver) // receiver(ft-contract) ? predecessor(bridge) ?
                        .setTransferTo(account_id)
                        .build()));
                break;
              }
              // {
              //   "gas": 100000000000000,
              //   "deposit": "1",
              //   "args_json": {
              //     "amount": "63442049574773100000",
              //     "recipient": "5d6c47cfdbbe5ad5a7be0a97d519164d303e1117"
              //   },
              //   "method_name": "withdraw"
              // }
              case "withdraw": {
                final String recipient = argsJson.get("recipient").getAsString();
                final String amount = argsJson.get("amount").getAsString();
                result.add(KeyValue.pair(predecessor, 
                    valueBuilder.apply(value)
                        .setAffectedAccount(predecessor)
                        .setAffectedAmount(new BigDecimal(amount).negate())
                        .setAffectedReason("withdraw")
                        .setTransferFrom(predecessor)
                        .setTransferTo(recipient)
                        .build()));
                break;
              }
              // {
              //   "gas": 5000000000000,
              //   "deposit": "1",
              //   "args_json": {
              //     "memo": null,
              //     "amount": "4116253556015596461",
              //     "receiver_id": "sbh2k.near"
              //   },
              //   "method_name": "ft_transfer"
              // }
              case "ft_transfer": {
                final String receiver_id = argsJson.get("receiver_id").getAsString();
                final String amount = argsJson.get("amount").getAsString();
                result.add(KeyValue.pair(predecessor, 
                    valueBuilder.apply(value)
                        .setAffectedAccount(predecessor)
                        .setAffectedAmount(new BigDecimal(amount).negate())
                        .setAffectedReason("ft_transfer_from")
                        .setTransferFrom(predecessor)
                        .setTransferTo(receiver_id)
                        .build()));
                result.add(KeyValue.pair(receiver_id, 
                    valueBuilder.apply(value)
                        .setAffectedAccount(receiver_id)
                        .setAffectedAmount(new BigDecimal(amount))
                        .setAffectedReason("ft_transfer_to")
                        .setTransferFrom(predecessor)
                        .setTransferTo(receiver_id)
                        .build()));
                break;
              }

              // case "ft_transfer_call": {
              //   final String receiver_id = argsJson.get("receiver_id").getAsString();
              //   final String amount = argsJson.get("amount").getAsString();
              //   result.add(KeyValue.pair(predecessor, 
              //       valueBuilder.apply(value)
              //           .setAffectedAccount(predecessor)
              //           .setAffectedAmount(new BigDecimal(amount).negate())
              //           .setAffectedReason("ft_transfer_call_from")
              //           .setTransferFrom(predecessor)
              //           .setTransferTo(receiver_id)
              //           .build()));
              //   result.add(KeyValue.pair(receiver_id, 
              //       valueBuilder.apply(value)
              //           .setAffectedAccount(receiver_id)
              //           .setAffectedAmount(new BigDecimal(amount))
              //           .setAffectedReason("ft_transfer_call_to")
              //           .setTransferFrom(predecessor)
              //           .setTransferTo(receiver_id)
              //           .build()));
              //   break;
              // }

              // {
              //   "gas": 5000000000000,
              //   "deposit": "0",
              //   "args_json": {
              //     "amount": "2500000000000000000000000",
              //     "sender_id": "louisliu.near",
              //     "receiver_id": "skyward.near"
              //   },
              //   "method_name": "ft_resolve_transfer"
              // }
              case "ft_resolve_transfer": {
                final String sender_id = argsJson.get("sender_id").getAsString();
                final String receiver_id = argsJson.get("receiver_id").getAsString();
                final String amount = argsJson.get("amount").getAsString();
                result.add(KeyValue.pair(sender_id, 
                    valueBuilder.apply(value)
                        .setAffectedAccount(sender_id)
                        .setAffectedAmount(new BigDecimal(amount).negate())
                        .setAffectedReason("ft_resolve_transfer_from")
                        .setTransferFrom(sender_id)
                        .setTransferTo(receiver_id)
                        .build()));
                result.add(KeyValue.pair(receiver_id, 
                    valueBuilder.apply(value)
                        .setAffectedAccount(receiver_id)
                        .setAffectedAmount(new BigDecimal(amount))
                        .setAffectedReason("ft_resolve_transfer_to")
                        .setTransferFrom(sender_id)
                        .setTransferTo(receiver_id)
                        .build()));
                break;
              }
          }
          return result;
        });

        tokenTransfer
            .peek((k, v) -> logger.debug("transfer: {} --> {} [{}] {}", v.getTransferFrom(), v.getTransferTo(), v.getAffectedReason(), v.getAffectedAmount()))
            // .map((key, value) -> KeyValue.pair(value.getAffectedAccount(), value)) // <account, transfer>
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
