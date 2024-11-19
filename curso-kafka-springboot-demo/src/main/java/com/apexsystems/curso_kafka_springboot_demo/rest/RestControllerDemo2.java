package com.apexsystems.curso_kafka_springboot_demo.rest;

import com.apexsystems.curso_kafka_springboot_demo.model.User;
import com.apexsystems.curso_kafka_springboot_demo.repo.IUserRepo;
import jakarta.validation.Valid;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/user")
public class RestControllerDemo2 {

    @Value("${url.proyect}")
    private String proyect;


    @Autowired
    private IUserRepo personaRepo;

    //PRACTICE 4.2
    @GetMapping
    public List<User> show(){
        System.out.println("List of Persons");
        return personaRepo.findAll();
    }

    //PRACTICE 4.3
    @PostMapping
    public String insert(@Valid @RequestBody User p){
        personaRepo.save(p);
        return "Person Added to the proyect: " + proyect;
    }

    //PRACTICE 4.2
    @PutMapping
    public String update(@RequestBody User p){
        personaRepo.update(p);
        return "Person Updated to the proyect: " + proyect;
    }

    //PRACTICE 4.2
    @DeleteMapping(value = "/{username}")
    public String delete(@PathVariable("username") String userName){
        personaRepo.deleteBy(userName);
        return "Person Deleted to the proyect: " + proyect;

    }

    //PRACTICE 4.2
    @GetMapping("/{username}/{fullname}/{age}")
    public User printObject(@PathVariable String username, @PathVariable String fullname, @PathVariable int age) {
        return new User(username,fullname, age);
    }
}
