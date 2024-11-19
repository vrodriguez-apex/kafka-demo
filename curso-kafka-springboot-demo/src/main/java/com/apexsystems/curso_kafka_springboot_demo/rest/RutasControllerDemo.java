package com.apexsystems.curso_kafka_springboot_demo.rest;

import com.apexsystems.curso_kafka_springboot_demo.model.Product;
import com.apexsystems.curso_kafka_springboot_demo.service.IProductService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

@RestController
public class RutasControllerDemo {

    @Autowired
    private IProductService productoService;

    //PRACTICE 3
    @GetMapping("/restctrl1")
    public String restCtrl1(){
        return "Mensaje con RestController";
    }

    //PRACTICE 3
    @GetMapping("/data/json")
    public Map<String, Object > userData(){
        return Map.of("user",Map.of(
                "name","Juan","age",32
        ));
    }

    //PRACTICE 3
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

    //PRACTICE 4.1
    @PostMapping("/register")
    public String registrarProductos(@RequestBody List<Product> product){
        productoService.save(product);
        return "saved products";
    }

    //PRACTICE 5
    @GetMapping("/division/{num}")
    public int checkCompute(@PathVariable int num) {
        if (num == 0) {
            throw new NullPointerException();
        }
        return (int)100 / num;
    }
}
