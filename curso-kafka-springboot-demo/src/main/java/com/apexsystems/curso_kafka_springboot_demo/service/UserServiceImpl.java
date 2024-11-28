package com.apexsystems.curso_kafka_springboot_demo.service;

import com.apexsystems.curso_kafka_springboot_demo.model.User;
import com.apexsystems.curso_kafka_springboot_demo.repo.IUserRepo;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class UserServiceImpl implements IUserRepo {
    //PRACTICE 4.2

    @Override
    public void save(User user) {
        System.out.println("Saving person");
    }

    @Override
    public List<User> findAll() {
        return List.of(new User("username_test", "full name_test",30));
    }

    @Override
    public void deleteBy(String userName) {
        System.out.println("Deleting person with username: " + userName);
    }

    @Override
    public void update(User user) {
        System.out.println("Updating person");
    }
}
