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
          logger.info("nanoseconds: {} --> milliseconds: {} [{}]", nanoseconds, milliseconds, this.name);
          return milliseconds;
        }
      };
    }
  }

  public static class Topics {

    public final static Map<String, Topic<?, ?>> ALL = new HashMap<>();
    // input topic
    public static Topic<String, abuda.indexer.receipts.Value> RECEIPTS;
    public static Topic<String, abuda.indexer.execution_outcomes.Value> EXECUTION_OUTCOMES;
    public static Topic<String, abuda.indexer.action_receipt_actions.Value> ACTION_RECEIPT_ACTIONS;
    // output topic
    public static Topic<String, abuda.indexer.token_balance.Value> TOKEN_BALANCE;

    static {
      createTopics();
    }

    private static void createTopics() {
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

      TOKEN_BALANCE = new Topic<>("near.indexer.token_balance", 
          Serdes.String(), new SpecificAvroSerde<>(), (v) -> {
            return v.getBlockTimestamp();
          });
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