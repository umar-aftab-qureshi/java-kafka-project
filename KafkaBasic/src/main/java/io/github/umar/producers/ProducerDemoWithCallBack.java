package io.github.umar.producers;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;


public class ProducerDemoWithCallBack {
    private static final Logger log = LoggerFactory.getLogger(ProducerDemoWithCallBack.class);

    public static void main(String[] args) {
        log.info("I'm a Kafka Producer with callBack");

        //create producer properties
        Properties properties = new Properties();
        // connect to local host
        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");
        // set producers properties
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());
        properties.setProperty("batch.size", "400"); // default size is 16
//        properties.setProperty("partitioner.class", RoundRobinPartitioner.class.getName());

        //create the producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        //create a producer record

//        for (int i = 0; i <= 40; i++) {
//            ProducerRecord<String, String> producerRecord =
//                    new ProducerRecord<>("demo_java", "Hello world " + i);
//
//            // send data
//            producer.send(producerRecord, new Callback() {
//                @Override
//                public void onCompletion(RecordMetadata metadata, Exception exception) {
//                    //execute everytime when msg sent successfully or exception thrown
//                    if (exception == null) {
//                        //msg send successfully
//                        log.info("Received new metadata \n" +
//                                "Topic:" + metadata.topic() + "\n" +
//                                "Partition:" + metadata.partition() + "\n" +
//                                "Offset:" + metadata.offset() + "\n" +
//                                "Timestamp:" + metadata.timestamp());
//                    } else {
//                        log.error("Error while producing:", exception);
//                    }
//                }
//            });
//        }

        for (int j = 0;j<=10;j++) {
            for (int i = 0; i <= 40; i++) {
                ProducerRecord<String, String> producerRecord =
                        new ProducerRecord<>("demo_java", "Hello world!!! i = " + i + " j= "+ j);

                // send data
                producer.send(producerRecord, new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata metadata, Exception exception) {
                        //execute everytime when msg sent successfully or exception thrown
                        if (exception == null) {
                            //msg send successfully
                            log.info("Received new metadata \n" +
                                    "Topic:" + metadata.topic() + "\n" +
                                    "Partition:" + metadata.partition() + "\n" +
                                    "Offset:" + metadata.offset() + "\n" +
                                    "Timestamp:" + metadata.timestamp());
                        } else {
                            log.error("Error while producing:", exception);
                        }
                    }
                });
            }
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
        // tell the producer to send all the data and block until done -- sync
        producer.flush();
        // flush and close the producer
        producer.close();
    }


}
