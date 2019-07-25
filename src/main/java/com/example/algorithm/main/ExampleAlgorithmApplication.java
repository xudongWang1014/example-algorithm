package com.example.algorithm.main;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;

@SpringBootApplication
@ComponentScan(basePackages = {"com.example.algorithm"})
public class ExampleAlgorithmApplication {

    public static void main(String[] args) {
        SpringApplication.run(ExampleAlgorithmApplication.class, args);
    }

}
