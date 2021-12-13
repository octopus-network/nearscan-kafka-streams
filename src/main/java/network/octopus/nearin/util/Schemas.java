package network.octopus.nearin.util;

import static io.confluent.kafka.schemaregistry.client.SchemaRegistryClientConfig.BASIC_AUTH_CREDENTIALS_SOURCE;
import static io.confluent.kafka.schemaregistry.client.SchemaRegistryClientConfig.USER_INFO_CONFIG;
import static io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;

import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.function.Function;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.processor.TimestampExtractor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Schemas {

  private static final Logger logger = LoggerFactory.getLogger(Schemas.class);

  public static class Topic<K, V> {

    private final String name;
    private final Serde<K> keySerde;
    private final Serde<V> valueSerde;
    private final Function<V, BigDecimal> timestampExtract;

    Topic(final String name, final Serde<K> keySerde, final Serde<V> valueSerde, final Function<V, BigDecimal> timestampExtract) {
      this.name = name;
      this.keySerde = keySerde;
      this.valueSerde = valueSerde;
      this.timestampExtract = timestampExtract;
      Topics.ALL.put(name, this);
    }

    public Serde<K> keySerde() {
      return keySerde;
    }

    public Serde<V> valueSerde() {
      return valueSerde;
    }

    public String name() {
      return name;
    }

    public String toString() {
      return name;
    }

    @SuppressWarnings("unchecked")
    public TimestampExtractor timestampExtractor() {
      return (ConsumerRecord<Object, Object> record, long partitionTime) -> {
        if (this.timestampExtract == null) {
          return partitionTime; //System.currentTimeMillis();
        } else {
          final BigDecimal nanoseconds = this.timestampExtract.apply((V)(record.value()));
          final long milliseconds = nanoseconds.divide(new BigDecimal(1e6), RoundingMode.HALF_UP).longValue();
          logger.debug("nanoseconds: {} --> milliseconds: {} [{}]", nanoseconds, milliseconds, this.name);
          return milliseconds;
        }
      };
    }
  }

  public static class Topics {

    public final static Map<String, Topic<?, ?>> ALL = new HashMap<>();
    // input topic
    public static Topic<String, near.indexer.receipts.Value> RECEIPTS;
    public static Topic<String, near.indexer.execution_outcomes.Value> EXECUTION_OUTCOMES;
    public static Topic<String, near.indexer.action_receipt_actions.Value> ACTION_RECEIPT_ACTIONS;
    public static Topic<String, near.indexer.data_receipts.Value> DATA_RECEIPTS;
    public static Topic<String, near.indexer.action_receipt_input_data.Value> ACTION_RECEIPT_INPUT_DATA;
    // output topic
    public static Topic<String, near.indexer.receipts_outcomes_actions.Value> RECEIPTS_OUTCOMES_ACTIONS;
    public static Topic<String, near.indexer.receipts_outcomes_actions_inputs.Value> RECEIPTS_OUTCOMES_ACTIONS_INPUTS;
    public static Topic<String, near.indexer.token_transfer.Value> TOKEN_TRANSFER;
    public static Topic<String, near.indexer.token_balance.Value> TOKEN_BALANCE;
    public static Topic<String, near.indexer.daily_activate_users.Value> TOKEN_DAILY_ACTIVATE_USERS;
    public static Topic<String, near.indexer.daily_transfer_topk.Value> TOKEN_DAILY_TRANSFER_TOPK;
    public static Topic<String, near.indexer.daily_holders.Value> TOKEN_DAILY_HOLDERS;

    static {
      createTopics();
    }

    private static void createTopics() {
      // input
      RECEIPTS = new Topic<>("near.indexer.receipts",
          Serdes.String(), new SpecificAvroSerde<>(), (v) -> {
            return v.getIncludedInBlockTimestamp();
          });
      EXECUTION_OUTCOMES = new Topic<>("near.indexer.execution_outcomes", 
          Serdes.String(), new SpecificAvroSerde<>(), (v) -> {
            return v.getExecutedInBlockTimestamp();
          });
      ACTION_RECEIPT_ACTIONS = new Topic<>("near.indexer.action_receipt_actions", 
          Serdes.String(), new SpecificAvroSerde<>(), (v) -> {
            return v.getReceiptIncludedInBlockTimestamp();
          });
      DATA_RECEIPTS = new Topic<>("near.indexer.data_receipts", 
          Serdes.String(), new SpecificAvroSerde<>(), null);

      ACTION_RECEIPT_INPUT_DATA = new Topic<>("near.indexer.action_receipt_input_data", 
          Serdes.String(), new SpecificAvroSerde<>(), null);
      
      // output
      RECEIPTS_OUTCOMES_ACTIONS = new Topic<>("near.indexer.receipts_outcomes_actions", 
          Serdes.String(), new SpecificAvroSerde<>(), (v) -> {
            return v.getReceipt().getIncludedInBlockTimestamp();
          });

      RECEIPTS_OUTCOMES_ACTIONS_INPUTS = new Topic<>("near.indexer.receipts_outcomes_actions_inputs", 
          Serdes.String(), new SpecificAvroSerde<>(), (v) -> {
            return v.getReceipt().getIncludedInBlockTimestamp();
          });

      TOKEN_TRANSFER = new Topic<>("near.indexer.token_transfer", 
          Serdes.String(), new SpecificAvroSerde<>(), (v) -> {
            return v.getIncludedInBlockTimestamp();
          });

      TOKEN_BALANCE = new Topic<>("near.indexer.token_balance", 
          Serdes.String(), new SpecificAvroSerde<>(), (v) -> {
            return v.getBlockTimestamp();
          });

      TOKEN_DAILY_ACTIVATE_USERS = new Topic<>("near.indexer.token_daily_activate_users", 
          Serdes.String(), new SpecificAvroSerde<>(), null);

      TOKEN_DAILY_TRANSFER_TOPK = new Topic<>("near.indexer.token_daily_transfer_topk", 
          Serdes.String(), new SpecificAvroSerde<>(), null);

      TOKEN_DAILY_HOLDERS = new Topic<>("near.indexer.token_daily_holders", 
          Serdes.String(), new SpecificAvroSerde<>(), null);
    }
  }

  public static Map<String, ?> buildSchemaRegistryConfigMap(final Properties config) {
    final HashMap<String, String> map = new HashMap<>();
    if (config.containsKey(SCHEMA_REGISTRY_URL_CONFIG))
      map.put(SCHEMA_REGISTRY_URL_CONFIG, config.getProperty(SCHEMA_REGISTRY_URL_CONFIG));
    if (config.containsKey(BASIC_AUTH_CREDENTIALS_SOURCE))
      map.put(BASIC_AUTH_CREDENTIALS_SOURCE, config.getProperty(BASIC_AUTH_CREDENTIALS_SOURCE));
    if (config.containsKey(USER_INFO_CONFIG))
      map.put(USER_INFO_CONFIG, config.getProperty(USER_INFO_CONFIG));
    return map;
  }

  public static void configureSerdes(final Properties config) {
    Topics.createTopics(); //wipe cached schema registry
    for (final Topic<?, ?> topic : Topics.ALL.values()) {
      configureSerde(topic.keySerde(), config, true);
      configureSerde(topic.valueSerde(), config, false);
    }
  }

  private static void configureSerde(final Serde<?> serde, final Properties config, final Boolean isKey) {
    if (serde instanceof SpecificAvroSerde) {
      serde.configure(buildSchemaRegistryConfigMap(config), isKey);
    }
  }
}