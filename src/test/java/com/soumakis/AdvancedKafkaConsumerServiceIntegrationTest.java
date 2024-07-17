package com.soumakis;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer.OrderAnnotation;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.MessageListener;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

@ExtendWith(SpringExtension.class)
@SpringBootTest
@EmbeddedKafka(partitions = 2, topics = {"test_topic"})
@TestMethodOrder(OrderAnnotation.class)
class AdvancedKafkaConsumerServiceIntegrationTest {

  private static KafkaContainer kafkaContainer;

  @Autowired
  private KafkaTemplate<String, String> kafkaTemplate;

  @Autowired
  private AdvancedKafkaConsumerService advancedKafkaConsumerService;

  @Autowired
  private ConsumerFactory<String, String> consumerFactory;

  @BeforeAll
  static void setUp() {
    kafkaContainer = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:latest"));
    kafkaContainer.start();
    System.setProperty("spring.kafka.bootstrap-servers", kafkaContainer.getBootstrapServers());
  }

  @AfterAll
  static void tearDown() {
    kafkaContainer.stop();
  }

  @Test
  void testConsumerInitialization() {
    assertThat(advancedKafkaConsumerService).isNotNull();
  }

  @Test
  @Order(1)
  void testBasicMessageConsumption() throws Exception {
    kafkaTemplate.send("test_topic", "key", "value");
    kafkaTemplate.flush();
    Thread.sleep(5000); // Wait for the message to be consumed

    long lag = advancedKafkaConsumerService.getLag();
    assertThat(lag).isEqualTo(0);
  }

  @Test
  @Order(2)
  void testLagCalculation() throws Exception {
    kafkaTemplate.send("test_topic", "key", "value1");
    kafkaTemplate.send("test_topic", "key", "value2");
    kafkaTemplate.flush();
    Thread.sleep(5000); // Wait for messages to be consumed

    long lag = advancedKafkaConsumerService.getLag();
    assertThat(lag).isEqualTo(0); // Assuming all messages are consumed
  }

  @Test
  @Order(3)
  void testRebalanceHandling() throws Exception {
    // Configure a second consumer to force a rebalance
    Map<String, Object> consumerProps = consumerProps();
    DefaultKafkaConsumerFactory<String, String> consumerFactory = new DefaultKafkaConsumerFactory<>(
        consumerProps);
    ContainerProperties containerProps = new ContainerProperties("test_topic");
    containerProps.setMessageListener((MessageListener<String, String>) message -> {
    });
    ConcurrentMessageListenerContainer<String, String> container =
        new ConcurrentMessageListenerContainer<>(consumerFactory, containerProps);

    container.start();

    kafkaTemplate.send("test_topic", "key", "value");
    kafkaTemplate.flush();
    Thread.sleep(5000); // Wait for rebalance and message consumption

    long lag = advancedKafkaConsumerService.getLag();
    assertThat(lag).isEqualTo(0);

    container.stop();
  }

  @Test
  @Order(4)
  void testMultipleRebalances() throws Exception {
    // Configure a second consumer to force a rebalance
    Map<String, Object> consumerProps = consumerProps();
    DefaultKafkaConsumerFactory<String, String> consumerFactory = new DefaultKafkaConsumerFactory<>(
        consumerProps);
    ContainerProperties containerProps = new ContainerProperties("test_topic");
    containerProps.setMessageListener((MessageListener<String, String>) message -> {
    });
    ConcurrentMessageListenerContainer<String, String> container =
        new ConcurrentMessageListenerContainer<>(consumerFactory, containerProps);

    container.start();
    kafkaTemplate.send("test_topic", "key", "value1");
    kafkaTemplate.send("test_topic", "key", "value2");
    kafkaTemplate.flush();

    Thread.sleep(5000); // Wait for rebalance and message consumption

    container.stop();
    Thread.sleep(5000);

    kafkaTemplate.send("test_topic", "key", "value3");
    kafkaTemplate.flush();
    Thread.sleep(5000); // Wait for messages to be consumed

    long lag = advancedKafkaConsumerService.getLag();
    assertThat(lag).isEqualTo(0);
  }

  @Test
  @Order(5)
  void testGracePeriodExpiry() throws Exception {
    kafkaTemplate.send("test_topic", "key", "value");
    kafkaTemplate.flush();
    Thread.sleep(30000); // Wait for the grace period to expire

    assertThat(
        advancedKafkaConsumerService.isLagStable()).isTrue();
  }

  @Test
  @Order(6)
  void testHandlingOfInvalidMessages() throws Exception {
    kafkaTemplate.send("test_topic", "key", null); // Sending a null value (invalid message)
    kafkaTemplate.flush();
    Thread.sleep(5000);

    long lag = advancedKafkaConsumerService.getLag();
    assertThat(lag).isEqualTo(0);
  }

  @Test
  @Order(9)
  void testConsumerWakeup() throws Exception {
    advancedKafkaConsumerService.shutdown();
    kafkaTemplate.send("test_topic", "key", "value");
    kafkaTemplate.flush();

    // Restart the consumer service
    advancedKafkaConsumerService = new AdvancedKafkaConsumerService(consumerFactory);
    advancedKafkaConsumerService.init();
    Thread.sleep(5000); // Wait for message to be processed

    long lag = advancedKafkaConsumerService.getLag();
    assertThat(lag).isEqualTo(0); // Ensure all messages are consumed after restart
  }

  @Test
  @Order(8)
  void testHighVolumeOfMessages() throws Exception {
    for (int i = 0; i < 10; i++) {
      kafkaTemplate.send("test_topic", "key" + i, "value" + i);
    }
    kafkaTemplate.flush();
    Thread.sleep(25000); // Wait for all messages to be consumed

    long lag = advancedKafkaConsumerService.getLag();
    assertThat(lag).isEqualTo(0);
  }

  @Test
  @Order(7)
  void testNoMessageConsumption() throws Exception {
    Thread.sleep(5000);

    assertThat(advancedKafkaConsumerService.getNonConsumingPolls()).isGreaterThan(0);
  }

  private Map<String, Object> consumerProps() {
    Map<String, Object> props = new HashMap<>(
        KafkaTestUtils.consumerProps(kafkaContainer.getBootstrapServers(), "advancedKafkaExample",
            "true"));
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaContainer.getBootstrapServers());
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    return props;
  }
}
