package com.example.kafkaproject.SpringApplicationKafka.component;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Headers;
import org.springframework.stereotype.Component;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

@Component
public class KafkaConsumer {

    @KafkaListener(topics = "${kafka.app.topic.name}", groupId = "${spring.kafka.consumer.group-id-webactivity}")
    public void consumeWebActivity(ConsumerRecord<String,String> message,@Headers Map<String, Object> headers) throws IOException {
        String key = message.key();
        String url = message.value();

        this.writeFile("web-activity.log", key + " - " + url);
    }

    @KafkaListener(topics = "${kafka.app.topic.name}", groupId = "${spring.kafka.consumer.group-id-webactivitydata}")
    public void consumeWebActivityData(ConsumerRecord<String,String> message,@Headers Map<String, Object> headers) throws IOException {
        String key = message.key();
        String url = message.value();

        Map<String, String> data = new HashMap();
        data.put("method",key);
        data.put("url",url);
        if(headers.get("userData") != null)
            data.put("userDataFromGet",new String((byte[]) headers.get("userData"), StandardCharsets.UTF_8));
        headers.forEach((key_h, value) -> {
            if(value instanceof byte[])
                data.put(key_h,new String((byte[]) value, StandardCharsets.UTF_8));
            else
                data.put(key_h,value.toString());
        });

        ObjectMapper objectMapper = new ObjectMapper();
        String jacksonData = objectMapper.writeValueAsString(data);

        this.writeFile("web-activity-data.txt", jacksonData);

    }

    public void writeFile(String filename, String content) throws IOException {
        File file = new File(filename);
        FileWriter fr = new FileWriter(file, true);
        BufferedWriter br = new BufferedWriter(fr);
        PrintWriter pr = new PrintWriter(br);
        pr.println(content);
        pr.close();
        br.close();
        fr.close();

        System.out.println("Successfully wrote to log file");

    }
}