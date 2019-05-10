package com.sclience.producer;

import org.springframework.context.support.ClassPathXmlApplicationContext;

import java.io.IOException;

public class ProducerDemo {
    public static void main(String[] args) throws IOException {
        ClassPathXmlApplicationContext ctx = new ClassPathXmlApplicationContext("spring-dubbo-producer.xml");
        ctx.start();
        System.in.read();
    }
}

