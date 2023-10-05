package com.microservices.nithin.twitter.to.kafka.service.runner.impl;


import com.microservices.nithin.twitter.to.kafka.service.runner.StreamRunner;
import com.microservices.nithin.demo.TwitterToKafkaServiceConfigData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;
import twitter4j.TwitterException;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Component
@ConditionalOnProperty(name = "twitter-to-kafka-service.enable-v2-tweets",havingValue = "true",matchIfMissing = true)
@ConditionalOnExpression("${twitter-to-kafka-service.enable-v2-tweets} && not ${twitter-to-kafka-service.enable-mock-tweets}")
public class TwitterV2KafkaStreamRunner implements StreamRunner {


    private static Logger LOGGER= LoggerFactory.getLogger(TwitterV2KafkaStreamRunner.class);

    private TwitterToKafkaServiceConfigData twitterToKafkaServiceConfigData;

    private TwitterV2StreamHelper twitterV2StreamHelper;


    public TwitterV2KafkaStreamRunner(TwitterToKafkaServiceConfigData twitterToKafkaServiceConfigData, TwitterV2StreamHelper twitterV2StreamHelper) {
        this.twitterToKafkaServiceConfigData = twitterToKafkaServiceConfigData;
        this.twitterV2StreamHelper = twitterV2StreamHelper;
    }

    @Override
    public void start()  {
        String bearerToken=twitterToKafkaServiceConfigData.getTwitterV2BearerToken();
        if(null!=bearerToken)
        {
            try {
                twitterV2StreamHelper.setupRules(bearerToken, getRules());
                twitterV2StreamHelper.connectStream(bearerToken);
            } catch (IOException | URISyntaxException | TwitterException e) {
//
                LOGGER.error("Error Streaming tweets!",e);
                throw new RuntimeException("Error Streaming tweets!",e);
            }
        }
        else {
            LOGGER.error("There was problem getting bearer token");
            throw new RuntimeException("There was problem getting bearer token");
        }

    }

    private Map<String, String> getRules() {
        List<String> keyWords=twitterToKafkaServiceConfigData.getTwitterKeywords();
        Map<String,String> rules= new HashMap<>();
        for(String keyword:keyWords)
        {
            rules.put(keyword,"Keyword: "+keyword);
        }
        LOGGER.info("Created filter for Twitetr for KeyWords:{}",keyWords);
        return rules;
    }
}
