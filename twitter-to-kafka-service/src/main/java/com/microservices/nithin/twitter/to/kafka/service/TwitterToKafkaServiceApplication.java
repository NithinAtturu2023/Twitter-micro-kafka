package com.microservices.nithin.twitter.to.kafka.service;

import com.microservices.nithin.twitter.to.kafka.service.init.StreamInitializer;
import com.microservices.nithin.twitter.to.kafka.service.runner.StreamRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;

@SpringBootApplication
@ComponentScan(basePackages = "com.microservices.nithin")
public class TwitterToKafkaServiceApplication implements CommandLineRunner {

    private static final Logger LOG=LoggerFactory.getLogger(TwitterToKafkaServiceApplication.class);

    private final StreamInitializer streamInitializer;


    private StreamRunner streamRunner;

    @Autowired
    public TwitterToKafkaServiceApplication(StreamInitializer streamInitializer, StreamRunner streamRunner) {
        this.streamInitializer = streamInitializer;
        this.streamRunner=streamRunner;
    }


    public static void main(String[] args) {
        SpringApplication.run(TwitterToKafkaServiceApplication.class,args);
    }

    @Override
    public void run(String... args) throws Exception {
        LOG.info("Application Started");
        streamInitializer.init();
        streamRunner.start();
    }
}
