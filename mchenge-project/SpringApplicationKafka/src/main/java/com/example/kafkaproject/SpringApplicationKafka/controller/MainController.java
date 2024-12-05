package com.example.kafkaproject.SpringApplicationKafka.controller;

import com.example.kafkaproject.SpringApplicationKafka.component.KafkaProducer;
import com.example.kafkaproject.SpringApplicationKafka.model.User;
import jakarta.servlet.http.HttpServletRequest;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.ModelAttribute;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;

import java.util.Arrays;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;

@Controller
public class MainController {

    private final KafkaProducer kafkaProducer;

    public MainController(KafkaProducer kafkaProducer){
        this.kafkaProducer = kafkaProducer;
    }

    @GetMapping("/")
    public String inicio(HttpServletRequest request){

        Map<String, String> map = new HashMap<String, String>();

        Enumeration headerNames = request.getHeaderNames();
        while (headerNames.hasMoreElements()) {
            String key = (String) headerNames.nextElement();
            String value = request.getHeader(key);
            map.put(key, value);
        }

        String headers = map.toString();

        System.out.println(headers);

        kafkaProducer.sendMessage(
                request.getMethod().toString(),
                headers,
                Arrays.toString(request.getCookies()),
                request.getRequestURL().toString(),
                request.getContextPath()
        );

        return "index";
    }

    @PostMapping("/formSubmit")
    public String submit(HttpServletRequest request,@RequestParam("username") String username, @RequestParam("fullname") String fullname, @RequestParam("age") int age){
        Map<String, String> map = new HashMap<String, String>();

        Enumeration headerNames = request.getHeaderNames();
        while (headerNames.hasMoreElements()) {
            String key = (String) headerNames.nextElement();
            String value = request.getHeader(key);
            map.put(key, value);
        }

        String headers = map.toString();

        System.out.println(headers);

        kafkaProducer.sendMessage(
                request.getMethod().toString(),
                headers,
                Arrays.toString(request.getCookies()),
                request.getRequestURL().toString(),
                request.getContextPath(),
                String.format("Username: %s, Fullname: %s Age %d",username,fullname,age)
        );
        return "test";
    }


}
