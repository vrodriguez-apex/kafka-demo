package com.example.kafkaproject.SpringApplicationKafka.model;

public class User {

    private String username;
    private String fullname;
    private int age;

    public User(String username, String fullname, Integer age) {
        this.username = username;
        this.fullname = fullname;
        this.age = age;
    }

    public int getAge() {
        return age;
    }

    public void setAge(int age) {
        this.age = age;
    }

    public String getFullname() {
        return fullname;
    }

    public void setFullname(String fullname) {
        this.fullname = fullname;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }
}

