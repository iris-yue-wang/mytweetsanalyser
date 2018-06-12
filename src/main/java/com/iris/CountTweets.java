package com.iris;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueStore;
import org.json.JSONObject;

import java.util.Properties;

public class CountTweets {
    public static void main(final String[] args) throws Exception {
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "tweet-count-by-user-analyser");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> tweets = builder.stream("tweets");
        KTable<String, Long> tweetCountByUser = tweets
                .filterNot((key, tweet) -> tweet.contains("delete"))
                .filter((key, tweet) -> tweet.contains("user"))
                .groupBy((key, tweet) -> {
                    JSONObject tweetJson = new JSONObject(tweet);
                    return tweetJson.getJSONObject("user").getString("screen_name");
                })
                .count();
        tweetCountByUser.toStream().to("tweetCountByUser", Produced.with(Serdes.String(), Serdes.Long()));

        KafkaStreams streams = new KafkaStreams(builder.build(), config);
        streams.start();
    }
}
