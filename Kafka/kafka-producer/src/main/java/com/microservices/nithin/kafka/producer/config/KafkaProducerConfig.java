package com.microservices.nithin.kafka.producer.config;

import com.microservices.nithin.demo.KafkaConfigData;
import com.microservices.nithin.demo.KafkaProducerConfigData;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaProducerConfig<K extends Serializable,V extends SpecificRecordBase> {

    private final KafkaConfigData kafkaConfigData;

    private final KafkaProducerConfigData kafkaProducerConfigData;

    public KafkaProducerConfig(KafkaConfigData kafkaConfigData, KafkaProducerConfigData kafkaProducerConfigData) {
        this.kafkaConfigData = kafkaConfigData;
        this.kafkaProducerConfigData = kafkaProducerConfigData;
    }


    @Bean
    public Map<String, Object>produceConfig(){
        Map<String, Object> props=new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,kafkaConfigData.getBootStrapServers());
        props.put(kafkaConfigData.getSchemaRegistryUrlKey(),kafkaConfigData.getSchemaRegistryUrl());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,kafkaProducerConfigData.getKeySerializerClass());
        props.put(ProducerConfig.BATCH_SIZE_CONFIG,kafkaProducerConfigData.getBatchSize()*kafkaProducerConfigData.getBatchSize());
        props.put(ProducerConfig.LINGER_MS_CONFIG,kafkaProducerConfigData.getLingerMs());
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG,kafkaProducerConfigData.getCompressionType());
        props.put(ProducerConfig.ACKS_CONFIG,kafkaProducerConfigData.getAcks());
        return props;
    }

    @Bean
    public ProducerFactory<K,V> producerFactory(){
        return new DefaultKafkaProducerFactory<>(produceConfig());
    }

    public KafkaTemplate<K,V>kafkaTemplate(){
        return new KafkaTemplate<>(producerFactory());
    }
}
