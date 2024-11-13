package org.exercises;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.Collections;
import java.util.Properties;
import java.util.Random;
import java.util.Scanner;
import java.util.concurrent.ExecutionException;

public class RandIntProducer {
    public static void main(String[] args) {
        String server = "localhost:9092";
        String topic = "rand-int-topic";

        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        try (final AdminClient adminClient = AdminClient.create(props)) {
            try {
                NewTopic newTopic = new NewTopic(topic, 1, (short)1);
                final CreateTopicsResult createTopicsResult = adminClient.createTopics(Collections.singleton(newTopic));
                createTopicsResult.values().get(topic).get();
            } catch (InterruptedException | ExecutionException e) {
                if (!(e.getCause() instanceof TopicExistsException))
                    throw new RuntimeException(e.getMessage(), e);
            }
        }

        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, server);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());

        KafkaProducer<String, Integer> producer = new KafkaProducer<>(properties);
        Random random = new Random();

        while (true) {
            try {
                Integer number = random.nextInt(1000);
                ProducerRecord<String, Integer> producerRecord =
                        new ProducerRecord<>(topic, number);
                producer.send(producerRecord);

                Thread.sleep(5000);
            } catch (InterruptedException e) {
                producer.flush();
                producer.close();
                throw new RuntimeException(e);
            }
        }
    }
}
