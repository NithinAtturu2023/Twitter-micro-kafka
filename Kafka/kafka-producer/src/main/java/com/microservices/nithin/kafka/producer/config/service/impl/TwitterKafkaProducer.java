package com.microservices.nithin.kafka.producer.config.service.impl;

import com.microservices.nithin.kafka.avro.model.TwitterAvroModel;
import com.microservices.nithin.kafka.producer.config.service.KafkaProducer;
import jakarta.annotation.PreDestroy;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.web.servlet.ServletComponentScan;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;
import org.springframework.util.concurrent.ListenableFuture;

import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;

@Service
public class TwitterKafkaProducer implements KafkaProducer<Long, TwitterAvroModel> {
    
    private static final Logger LOGGER= LoggerFactory.getLogger(TwitterKafkaProducer.class);
    
    private KafkaTemplate<Long,TwitterAvroModel> kafkaTemplate;

    public TwitterKafkaProducer(KafkaTemplate<Long, TwitterAvroModel> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    @Override
    public void send(String topicName, Long key, TwitterAvroModel message) {
        LOGGER.info("Sending message='{}'  to topic= '{}'",message,topicName);
        CompletableFuture<SendResult<Long, TwitterAvroModel>> kafkaResultFuture = kafkaTemplate.send(topicName, key, message);
        kafkaResultFuture.whenComplete(getCallback(topicName, message));
    }

    @PreDestroy
    public void close()
    {
        if(kafkaTemplate!=null)
        {
            LOGGER.info("Closing kafka producer");
            kafkaTemplate.destroy();
        }
    }

    private BiConsumer<? super SendResult<Long, TwitterAvroModel>,? super Throwable> getCallback(String topicName, TwitterAvroModel message) {
        return (result, ex) -> {
            if (ex == null) {
                RecordMetadata metadata = result.getRecordMetadata();
                LOGGER.info("Received new metadata. Topic: {}; Partition {}; Offset {}; Timestamp {}, at time {}",
                        metadata.topic(),
                        metadata.partition(),
                        metadata.offset(),
                        metadata.timestamp(),
                        System.nanoTime());
            } else {
                LOGGER.error("Error while sending message {} to topic {}", message.toString(), topicName, ex);
            }
        };
    }
}
