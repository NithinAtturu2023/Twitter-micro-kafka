package com.microservices.nithin.twitter.to.kafka.service.runner.impl;

import com.microservices.nithin.twitter.to.kafka.service.listener.TwitterKafkaStatusListener;
import com.microservices.nithin.twitter.to.kafka.service.runner.StreamRunner;
import com.microservices.nithin.demo.TwitterToKafkaServiceConfigData;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.stereotype.Component;
import twitter4j.FilterQuery;
import twitter4j.TwitterException;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;

import java.util.Arrays;

@Component
@ConditionalOnExpression("${twitter-to-kafka-service.enable-mock-tweets} && not ${twitter-to-kafka-service.enable-v2-tweets}")
public class TwitterKafkaStreamRunner implements StreamRunner {

    private static Logger LOGGER= LoggerFactory.getLogger(TwitterKafkaStreamRunner.class);

    private TwitterToKafkaServiceConfigData twitterToKafkaServiceConfigData;

    private TwitterKafkaStatusListener twitterKafkaStatusListener;

    private TwitterStream twitterStream;

    public TwitterKafkaStreamRunner(TwitterToKafkaServiceConfigData twitterToKafkaServiceConfigData,
                                    TwitterKafkaStatusListener twitterKafkaStatusListener) {
        this.twitterToKafkaServiceConfigData = twitterToKafkaServiceConfigData;
        this.twitterKafkaStatusListener = twitterKafkaStatusListener;
    }

    @Override
    public void start() throws TwitterException {
            twitterStream= new TwitterStreamFactory().getInstance();
            twitterStream.addListener(twitterKafkaStatusListener);
            String[] keywords=twitterToKafkaServiceConfigData.getTwitterKeywords().toArray(new String[0]);
            FilterQuery filterQuery= new FilterQuery(keywords);
            twitterStream.filter(filterQuery);
            LOGGER.info("Started filtering twitter stream for keywords {}", Arrays.toString(keywords));
    }

    @PreDestroy
    public void shutdown(){
        if(twitterStream!=null)
        {
            LOGGER.info("Closing twitter Stream");
            twitterStream.shutdown();
        }
    }

}
