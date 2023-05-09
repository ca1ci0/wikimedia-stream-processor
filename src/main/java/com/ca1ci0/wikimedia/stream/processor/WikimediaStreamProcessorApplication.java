package com.ca1ci0.wikimedia.stream.processor;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.EnableKafkaStreams;

@EnableKafkaStreams
@SpringBootApplication
public class WikimediaStreamProcessorApplication {

    public static void main(String[] args) {
        SpringApplication.run(WikimediaStreamProcessorApplication.class, args);
    }

}
