package com.apexsystems.curso_kafka_springboot_demo.service;

import com.apexsystems.curso_kafka_springboot_demo.model.Product;

import java.util.List;

public interface IProductService {
    //PRACTICE 4.1
    void save(List<Product> products);
}
