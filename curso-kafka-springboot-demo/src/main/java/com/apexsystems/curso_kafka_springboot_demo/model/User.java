package com.apexsystems.curso_kafka_springboot_demo.model;


import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;


public class User {
    //PRACTICE 4.1 4.3
    @NotNull
    private String userName;

    @NotBlank
    @JsonProperty("full_name")
    private String fullName;

    @Positive
    private int age;

    @JsonGetter("info")
    private String info(){
        return "The full name of the person is " + fullName;
    }

    public User(String userName, String fullName, int age) {
        this.userName = userName;
        this.fullName = fullName;
        this.age = age;
    }

    public String getUserName() {
        return userName;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }

    public String getFullName() {
        return fullName;
    }

    public void setFullName(String fullName) {
        this.fullName = fullName;
    }

    public int getAge() {
        return age;
    }

    public void setAge(int age) {
        this.age = age;
    }
}
