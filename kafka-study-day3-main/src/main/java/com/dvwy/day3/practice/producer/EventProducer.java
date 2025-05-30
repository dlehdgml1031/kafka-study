package com.dvwy.day3.practice.producer;

import org.apache.kafka.clients.producer.KafkaProducer;

import java.util.Properties;

public class EventProducer {
    private final String TOPIC = "web-raw-log";
    private KafkaProducer<String, String> producer;

    public EventProducer(Properties props){
        producer = new KafkaProducer<>(props);



    }

    public void send(String key, String jsonValue) {

    }
}


