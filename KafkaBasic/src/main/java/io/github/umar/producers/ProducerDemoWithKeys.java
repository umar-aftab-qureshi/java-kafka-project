package io.github.umar.producers;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;


public class ProducerDemoWithKeys {
    private static final Logger log = LoggerFactory.getLogger(ProducerDemoWithKeys.class);

    public static void main(String[] args) {
        log.info("I'm a Kafka Producer with Keys");

        //create producer properties
        Properties properties = new Properties();
        // connect to local host
        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");
        // set producers properties
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());


        //create the producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        //create a producer record


//        for (int i = 0; i <= 40; i++) {
//            String topic = "demo_java";
//            String key = "ID_" + i;
//            String value = "Hello world " + i;
//            ProducerRecord<String, String> producerRecord =
//                    new ProducerRecord<>(topic,key,value);
//
//            // send data
//            producer.send(producerRecord, new Callback() {
//                @Override
//                public void onCompletion(RecordMetadata metadata, Exception exception) {
//                    //execute everytime when msg sent successfully or exception thrown
//                    if (exception == null) {
//                        //msg send successfully
//                        log.info("Kay :" + key +"\n" +
//                                "Partition :" + metadata.partition());
//
//                    } else {
//                        log.error("Error while producing:", exception);
//                    }
//                }
//            });
//        }
        for (int j = 1; j < 10; j++) {
            for (int i = 0; i <= 40; i++) {
                String topic = "demo_java";
                String key = "ID_" + i;
                String value = "Hello world " + i;
                ProducerRecord<String, String> producerRecord =
                        new ProducerRecord<>(topic, key, value);

                // send data
                producer.send(producerRecord, new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata metadata, Exception exception) {
                        //execute everytime when msg sent successfully or exception thrown
                        if (exception == null) {
                            //msg send successfully
                            log.info("Kay :" + key + "\n" +
                                    "Partition :" + metadata.partition());

                        } else {
                            log.error("Error while producing:", exception);
                        }
                    }
                });
            }
        }


        // tell the producer to send all the data and block until done -- sync
        producer.flush();
        // flush and close the producer
        producer.close();
    }


}
