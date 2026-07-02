package io.github.umar.consumers;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;


public class ConsumerDemoWithShutDown {
    private static final Logger log = LoggerFactory.getLogger(ConsumerDemoWithShutDown.class);

    public static void main(String[] args) {
        log.info("I'm a Kafka Consumer with shutdown");
        String groupId = "my-java-application";
        String topic = "demo_java";

        //create consumer properties
        Properties properties = new Properties();
        // connect to local host
        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");
        // set consumer properties
        properties.setProperty("key.deserializer", StringDeserializer.class.getName());
        properties.setProperty("value.deserializer", StringDeserializer.class.getName());

        properties.setProperty("group.id", groupId);
        properties.setProperty("auto.offset.reset", "earliest"); //none --> if don't have group then fail  /earliest --> read from beginning / latest --> only read new messages


        // create consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        // get a reference to the main thread
        final Thread mainThread = Thread.currentThread();
        //adding the shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                log.info("Detected a shutdown, let's exit by calling  consumer.wakeup()....");
                consumer.wakeup();

                // join the main thread to allow the exeution of the main thread code
                try {
                    mainThread.join();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        });

        try {
            // Subscribe to the topic
            consumer.subscribe(Arrays.asList(topic));

            // poll for data or reterive data

            while (true) {
                log.info("******polling*******");

                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                for (ConsumerRecord<String, String> record : records) {
                    log.info("Key: " + record.key() + ", Value: " + record.value());
                    log.info("Partition: " + record.partition() + ", Offset: " + record.offset());
                }
            }
        } catch (WakeupException e) {
            log.info("Consumer is starting to shutdown...");
        } catch (Exception e) {
            log.error("Unexpected exception in the consumer", e);
        } finally {
            consumer.close(); // close the consumer and this will also commit the offset
            log.info("Consumer is now gracefully shutdown...");
        }


    }


}
