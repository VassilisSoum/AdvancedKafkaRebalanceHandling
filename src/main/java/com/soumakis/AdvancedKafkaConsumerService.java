package com.soumakis;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.stereotype.Service;

/**
 * A service that consumes messages from a Kafka topic and simulates processing time. It keeps track
 * of the lag for each partition and the overall lag while handling rebalances and partition
 * revocations.
 */
@Service
public class AdvancedKafkaConsumerService {

  private static final long GRACE_PERIOD_MS = 30000; // 30 seconds grace period
  private static final long HISTORY_RESET_PERIOD_MS = 120000; // 120 seconds to reset history
  private static final int MAX_NON_CONSUMING_POLLS = 5; // Number of consecutive polls with no consumption to consider lag stable

  private final KafkaConsumer<String, String> consumer;
  private final ScheduledExecutorService executorService;
  private final Map<Integer, Long> partitionLagMap = new ConcurrentHashMap<>();
  private final Queue<Long> lagHistory = new ConcurrentLinkedQueue<>();

  private long lastRebalanceTime = System.currentTimeMillis();
  private long lastHistoryResetTime = System.currentTimeMillis();
  private long lastConsumedTime = System.currentTimeMillis();
  private int nonConsumingPolls = 0;

  @Autowired
  public AdvancedKafkaConsumerService(ConsumerFactory<String, String> consumerFactory) {
    this.consumer = (KafkaConsumer<String, String>) consumerFactory.createConsumer(
        "advancedKafkaExample", null);
    this.executorService = Executors.newSingleThreadScheduledExecutor();
  }

  @PostConstruct
  void init() {
    consumer.subscribe(Collections.singletonList("test_topic"), new CustomRebalanceListener());
    startConsuming();
  }

  /**
   * Get the total lag across all partitions.
   *
   * @return the total lag
   */
  public long getLag() {
    return partitionLagMap.values().stream().mapToLong(Long::longValue).sum();
  }

  /**
   * Gets the non consuming polls count.
   *
   * @return the non consuming polls count
   */
  public int getNonConsumingPolls() {
    return nonConsumingPolls;
  }

  /**
   * Check if the lag is stable. The lag is considered stable if there are no messages consumed for
   * some time exceeded the configured grace period or if the number of tries to consume messages is
   * greater than the configured maximum.
   *
   * @return true if the lag is stable, false otherwise
   */
  public boolean isLagStable() {
    long now = System.currentTimeMillis();
    if (now - lastRebalanceTime < GRACE_PERIOD_MS) {
      System.out.println("In grace period, skipping lag stability check.");
      return false;
    } else if (now - lastConsumedTime > GRACE_PERIOD_MS) {
      System.out.println(
          "No messages consumed for longer than the grace period, considering lag stable.");
      return true;
    } else {
      return nonConsumingPolls >= MAX_NON_CONSUMING_POLLS
          || lagHistory.stream().distinct().count() == 1;
    }
  }

  @PreDestroy
  public void shutdown() {
    executorService.submit(() -> {
      consumer.wakeup();
      consumer.close();
      executorService.shutdown();
    });
  }

  /**
   * Poll records from the Kafka topic.
   */
  void pollRecords() {
    var records = consumer.poll(Duration.ofSeconds(3));
    processRecords(records);
  }

  /**
   * Reset the lag history periodically to keep memory usage low.
   */
  void resetLagHistoryPeriodically() {
    long now = System.currentTimeMillis();
    if (now - lastHistoryResetTime > HISTORY_RESET_PERIOD_MS) {
      lagHistory.clear();
      lastHistoryResetTime = now;
    }
  }

  /**
   * Get the number of elements in the lag history.
   *
   * @return the size of the lag history
   */
  int getLagHistorySize() {
    return lagHistory.size();
  }

  private void startConsuming() {
    executorService.scheduleAtFixedRate(this::pollRecords, 0, 3, TimeUnit.SECONDS);
    executorService.scheduleAtFixedRate(this::resetLagHistoryPeriodically, 0,
        HISTORY_RESET_PERIOD_MS,
        TimeUnit.MILLISECONDS);
  }

  /**
   * Process the records received from the Kafka topic. It simulates processing time, updates the
   * lag info and commits the offset.
   *
   * @param records the records to process
   */
  private void processRecords(Iterable<ConsumerRecord<String, String>> records) {
    if (!records.iterator().hasNext()) {
      nonConsumingPolls++;
    } else {
      nonConsumingPolls = 0;
      lastConsumedTime = System.currentTimeMillis();
    }

    for (ConsumerRecord<String, String> record : records) {
      System.out.println(
          "Received message: " + record.value() + " from partition: " + record.partition());
      simulateProcessingDelay();
      updateLagInfo(record);
      commitOffset(record);
    }
  }

  private void simulateProcessingDelay() {
    try {
      Thread.sleep(2000); // 2 seconds delay
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException(e);
    }
  }

  /**
   * Update the lag info for the given record. It calculates the lag for the partition and updates
   * the lag history.
   *
   * @param record the record to update the lag info for
   */
  private void updateLagInfo(ConsumerRecord<String, String> record) {
    int partition = record.partition();
    long currentOffset = record.offset();
    long latestOffset = consumer
        .endOffsets(Collections.singleton(new TopicPartition(record.topic(), partition)))
        .get(new TopicPartition(record.topic(), partition));
    long lag = latestOffset - currentOffset;
    partitionLagMap.put(partition, lag);

    // Add the lag to history
    if (lagHistory.size() >= 10) {
      lagHistory.poll();
    }
    lagHistory.offer(lag);
  }

  /**
   * Commits the offset for the given record and updates the lag info.
   *
   * @param record the record to commit the offset for
   */
  private void commitOffset(ConsumerRecord<String, String> record) {
    consumer.commitSync(Collections.singletonMap(
        new TopicPartition(record.topic(), record.partition()),
        new OffsetAndMetadata(record.offset() + 1)
    ));

    partitionLagMap.computeIfPresent(record.partition(), (k, v) -> {
      var x = v - 1;
      if (x < 0) {
        return 0L;
      }
      return x;
    });
  }

  private class CustomRebalanceListener implements ConsumerRebalanceListener {

    /**
     * Commit the offsets before rebalancing. This is important to avoid processing the same message
     * multiple times.
     *
     * @param partitions the partitions to commit the offsets for
     */
    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
      System.out.println("Committing offsets before rebalancing...");
      consumer.commitSync();

      partitions.forEach(partition -> partitionLagMap.remove(partition.partition()));
    }

    /**
     * Reset the lag history and partition lag map after rebalancing. This is important to avoid
     * stale lag info.
     *
     * @param partitions the partitions to reset the lag info for
     */
    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
      System.out.println("Rebalanced: " + partitions);
      lastRebalanceTime = System.currentTimeMillis();
      lagHistory.clear();
      partitionLagMap.clear();
    }
  }
}
