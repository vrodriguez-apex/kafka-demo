package com.example.kafkaproject.SpringApplicationKafka.component;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
public class KafkaProducer {

    private final KafkaTemplate<String, String> kafkaTemplate;

    @Value("${kafka.app.topic.name}")
    private String topic;

    public KafkaProducer(KafkaTemplate<String,String> kafkaTemplate){
        this.kafkaTemplate = kafkaTemplate;
    }

    public void sendMessage(String key, String headers,String cookies, String uri, String path){
        var record = new ProducerRecord<String, String>(this.topic, key, uri);
        record.headers().add("headers", headers.getBytes());
        record.headers().add("cookies", cookies.getBytes());
        record.headers().add("path", path.getBytes());

        System.out.println("Sending Data");

        kafkaTemplate.send(record);
    }

    public void sendMessage(String key, String headers,String cookies, String uri, String path, String opcionalData){
        var record = new ProducerRecord<String, String>(this.topic, key, uri);
        record.headers().add("headers", headers.getBytes());
        record.headers().add("cookies", cookies.getBytes());
        record.headers().add("path", path.getBytes());
        record.headers().add("userData", opcionalData.getBytes());

        System.out.println("Sending Data");

        kafkaTemplate.send(record);
    }


}
