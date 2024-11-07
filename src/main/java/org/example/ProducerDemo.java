package org.example;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.*;

public class ProducerDemo {
    public static void main(String[] args) {
        String server = "localhost:9092";
        String topic = "test-apex-topic";

        // create Producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, server);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // create the producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        LinkedList<String> rows = new LinkedList<>();
        rows.add("first row");
        rows.add("second row");
        rows.add("third row");

        for (String row : rows){
            // create a producer record
            ProducerRecord<String, String> producerRecord =
                    new ProducerRecord<>(topic, row);

            // send data - asynchronous
            producer.send(producerRecord);
        }

        // flush data - synchronous
        producer.flush();

        // flush and close producer
        producer.close();
    }
}
