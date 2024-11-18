package com.apexsystems.curso_kafka_springboot_demo.rest;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Map;

@RestController
public class RutasControllerDemo {
    @GetMapping("/restctrl1")
    public String restCtrl1(){
        return "Mensaje con RestController";
    }

    @GetMapping("/data/json")
    public Map<String, Object > userData(){
        return Map.of("user",Map.of(
                "name","Juan","age",32
        ));
    }

    @GetMapping("/data/json/v2")
    public Map<String, Object> userDatav2() {
        return Map.of(
                "users", List.of(
                        Map.of("name", "Juan", "age", 32),
                        Map.of("name", "Maria", "age", 28),
                        Map.of("name", "Carlos", "age", 40)
                )
        );
    }

}
