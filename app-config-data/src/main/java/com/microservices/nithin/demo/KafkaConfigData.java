package com.microservices.nithin.demo;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

import java.util.List;

@Data
@Configuration
@ConfigurationProperties(prefix = "kafka-config")
public class KafkaConfigData {

    private String bootStrapServers;

    private String schemaRegistryUrlKey;

    private String schemaRegistryUrl;

    private String topicName;

    private List<String> topicNamesToCreate;

    private Integer numOfPartitions;

    private Short replicationFactor;
}
