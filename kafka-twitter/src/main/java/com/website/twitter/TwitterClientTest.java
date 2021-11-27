package com.website.twitter;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class TwitterClientTest {

    public static String API_KEY = "8x1fLOuyMYhCiNQ3sugya1nTr";
    public static String API_SECRET_KEY = "AZjDbpYN6tk8GbciEKksPZk5o7AAoKIATwqnuo0YzaR9laTjV9";
    public static String ACCESS_TOKEN = "2407406035-wF13eG6TXctpgLTTQvfuJ792c0vawv9bMfgbsrc";
    public static String ACCESS_TOKEN_SECRET = "T0hST9zY8iWNXgzp34q7afVXkXT4DJWx4d5a4StlMcDUq";
    public static Logger logger = LoggerFactory.getLogger(TwitterClientTest.class);

    public TwitterClientTest() {
    }

    public static void main(String[] args) {
        new TwitterClientTest().run();
    }

    public void run() {
        logger.info("SETUP");
        /** Set up your blocking queues: Be sure to size these properly based on expected TPS of your stream */
        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(1000);

        // create a twitter client
        Client client = createTwitterClient(msgQueue);

        // Attempts to establish a connection.
        client.connect();

        // create a kafka producer


        // loop to send tweets to kafka
        // on a different thread, or multiple different threads....
        while (!client.isDone()) {
            String msg = null;
            try {
                msg = msgQueue.poll(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
                client.stop();
            }
            if (msg != null){
                logger.info(msg);
            }
        }

    }

    public Client createTwitterClient(BlockingQueue<String> msgQueue) {
        /** Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth) */
        Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();

        // Optional: set up some followings and track terms
        List<String> terms = Lists.newArrayList("bitcoin");
        hosebirdEndpoint.trackTerms(terms);

        // These secrets should be read from a config file
        Authentication hosebirdAuth = new OAuth1(API_KEY, API_SECRET_KEY, ACCESS_TOKEN, ACCESS_TOKEN_SECRET);

        ClientBuilder builder = new ClientBuilder()
                .name("Hosebird-Client-01")                              // optional: mainly for the logs
                .hosts(hosebirdHosts)
                .authentication(hosebirdAuth)
                .endpoint(hosebirdEndpoint)
                .processor(new StringDelimitedProcessor(msgQueue));

        return builder.build();
    }


}
