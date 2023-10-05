package com.microservices.nithin.twitter.to.kafka.service.transformer;

import com.microservices.nithin.kafka.avro.model.TwitterAvroModel;
import org.springframework.stereotype.Component;
import twitter4j.Status;

@Component
public class TwitterStatusToAvroTransformer {


    public TwitterAvroModel getTwitterAvroModelFromStatus(Status status)
    {
        return TwitterAvroModel
                .newBuilder()
                .setId(status.getId())
                .setUserId(status.getUser().getId())
                .setText(status.getText())
                .setCreatedAt(status.getCreatedAt().getTime())
                .build();
    }
}
