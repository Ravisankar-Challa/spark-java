package com.example.sparkexamples;

import java.io.Serializable;

@SuppressWarnings("serial")
public class Person implements Serializable {
    private String name;
    private long age;
    public Person() {}
    public Person(String name, long age) {
        super();
        this.name = name;
        this.age = age;
    }
    public String getName() {
        return name;
    }
    public void setName(String name) {
        this.name = name;
    }
    public long getAge() {
        return age;
    }
    public void setAge(long age) {
        this.age = age;
    }
}
