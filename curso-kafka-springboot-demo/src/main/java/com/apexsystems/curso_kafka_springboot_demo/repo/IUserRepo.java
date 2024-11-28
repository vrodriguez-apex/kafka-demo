package com.apexsystems.curso_kafka_springboot_demo.repo;

import com.apexsystems.curso_kafka_springboot_demo.model.User;

import java.util.List;

public interface IUserRepo {
    //PRACTICE 4.2
    public void save(User user);

    public List<User>  findAll();

    public void deleteBy(String userName);

    public void update(User user);
}
