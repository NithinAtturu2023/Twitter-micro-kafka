package com.microservices.nithin.twitter.to.kafka.service.runner.impl;

import com.microservices.nithin.twitter.to.kafka.service.exception.TwitterToKafkaServiceException;
import com.microservices.nithin.twitter.to.kafka.service.listener.TwitterKafkaStatusListener;
import com.microservices.nithin.twitter.to.kafka.service.runner.StreamRunner;
import com.microservices.nithin.demo.TwitterToKafkaServiceConfigData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;
import twitter4j.Status;
import twitter4j.TwitterException;
import twitter4j.TwitterObjectFactory;

import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Locale;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;

@Component
@ConditionalOnProperty(name = "twitter-to-kafka-service.enable-mock-tweets",havingValue = "true")
public class MockKafkaStreamRunner implements StreamRunner {

    private static final Logger LOGGER= LoggerFactory.getLogger(MockKafkaStreamRunner.class);

    private TwitterToKafkaServiceConfigData twitterToKafkaServiceConfigData;

    private TwitterKafkaStatusListener twitterKafkaStatusListener;

    private static final Random RANDOM= new Random();

    private static final String[] WORDS= new String[]{
            "Lorem",
            "ipsum",
            "dolor",
            "sit",
            "amet",
            "consectetuer",
            "adipiscing",
            "elit",
            "Maecenas",
            "porttitor",
            "congue",
            "massa",
            "Fusce",
            "posuere",
            "magna",
            "sed",
            "pulvinar",
            "ultricies",
            "purus",
            "lectus",
            "malesuada",
            "libero"
    };
    private static final String tweetAsRawJson = "{" +
            "\"created_at\":\"{0}\"," +
            "\"id\":\"{1}\"," +
            "\"text\":\"{2}\"," +
            "\"user\":{\"id\":\"{3}\"}" +
            "}";

    private static final String TWITTER_STATUS_DATE_FORMAT="EEE MMM dd HH:mm:ss zzz yyyy";

    public MockKafkaStreamRunner(TwitterToKafkaServiceConfigData twitterToKafkaServiceConfigData, TwitterKafkaStatusListener twitterKafkaStatusListener) {
        this.twitterToKafkaServiceConfigData = twitterToKafkaServiceConfigData;
        this.twitterKafkaStatusListener = twitterKafkaStatusListener;
    }

    @Override
    public void start() throws TwitterException {
        String[] keywords=twitterToKafkaServiceConfigData.getTwitterKeywords().toArray(new String[0]);
        int minTweetLength= twitterToKafkaServiceConfigData.getMockMinTweetLength();
        int maxTweetLength=twitterToKafkaServiceConfigData.getMockMaxTweetLength();
        long sleepTimems=twitterToKafkaServiceConfigData.getMockSleepMs();
        LOGGER.info("Starting mock filtering twitter streams for keywords {}", Arrays.toString(keywords));
        simulateTwitterStream(keywords, minTweetLength, maxTweetLength, sleepTimems);
    }

    private void simulateTwitterStream(String[] keywords, int minTweetLength, int maxTweetLength, long sleepTimems){
        Executors.newSingleThreadExecutor().submit(()->{
         try{
            while(true) {
            String formatterTwitterJson= getFormattedTweet(keywords, minTweetLength, maxTweetLength);
            Status status= TwitterObjectFactory.createStatus(formatterTwitterJson);
            twitterKafkaStatusListener.onStatus(status);
            sleep(sleepTimems);
            }
         }catch (TwitterException e) {
          LOGGER.error("Error Creating twitter Status!",e);
         }
         });
    }

    private void sleep(long sleepTimems) {
        try{
            Thread.sleep(sleepTimems);
        }catch (InterruptedException e)
        {
            throw new TwitterToKafkaServiceException( "Exception while sleeping for waiting new status to create!!");
        }
    }

    private String getFormattedTweet(String[] keywords, int minTweetLength, int maxTweetLength) {
        String[] params= new String[]{
                ZonedDateTime.now().format(DateTimeFormatter.ofPattern(TWITTER_STATUS_DATE_FORMAT, Locale.ENGLISH)),
                String.valueOf(ThreadLocalRandom.current().nextLong(Long.MAX_VALUE)),
                getRandomTweetContent(keywords,minTweetLength,maxTweetLength),
                String.valueOf(ThreadLocalRandom.current().nextLong(Long.MAX_VALUE))
        };
        return fomattedTweetAsJsonWithParams(params);
    }

    private static String fomattedTweetAsJsonWithParams(String[] params) {
        String tweet=tweetAsRawJson;
        for(int i = 0; i< params.length; i++)
        {
            tweet=tweet.replace("{"+i+"}", params[i]);
        }
        return tweet;
    }

    private String getRandomTweetContent(String[] keywords, int minTweetLength, int maxTweetLength) {
        StringBuilder tweet= new StringBuilder();
        int tweetLenght=RANDOM.nextInt(maxTweetLength-minTweetLength+1)+minTweetLength;
        for(int i=0;i<=tweetLenght;i++)
        {
            tweet.append(WORDS[RANDOM.nextInt(WORDS.length)]).append(" ");
            if(i==tweetLenght/2){
                tweet.append(keywords[RANDOM.nextInt(keywords.length)]).append(" ");
            }
        }
        return tweet.toString().trim();
    }
}