package com.apexsystems.curso_kafka_springboot_demo.service;

import com.apexsystems.curso_kafka_springboot_demo.model.Product;
import com.apexsystems.curso_kafka_springboot_demo.repo.IProductRepo;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class ProductServiceImpl implements IProductService {
    //PRACTICE 4.1

    @Autowired
    @Qualifier("product1")
    private IProductRepo productoRepo;

    @Override
    public void save(List<Product> products) {
        productoRepo.save(products);
    }
}
