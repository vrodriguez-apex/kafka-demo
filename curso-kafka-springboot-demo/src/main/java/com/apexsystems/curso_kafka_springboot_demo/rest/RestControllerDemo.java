package com.apexsystems.curso_kafka_springboot_demo.rest;

import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/personas")
public class RestControllerDemo {
    @GetMapping
    public String mostrarGetMapping(){
        return "Este es un mensaje desde RestController personas con la funcion: mostrarGetMapping";
    }

    @PostMapping
    public String mostrarPostMapping(){
        return "Este es un mensaje desde RestController personas con la funcion: mostrarPostMapping";
    }
}
