package com.apexsystems.curso_kafka_springboot_demo.repo;

import com.apexsystems.curso_kafka_springboot_demo.model.Product;

import java.util.List;

public interface IProductRepo {
    //PRACTICE 4.1
    void save(List<Product> products);
}
