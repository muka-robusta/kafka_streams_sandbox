package com.github.one2story.kafka.streams;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.GlobalKTable;
import org.apache.kafka.streams.kstream.KStream;

import java.util.Properties;

public class UserEventEnricherApp {
    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "user-event-enricher-app");
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        StreamsBuilder builder = new StreamsBuilder();

        GlobalKTable<String, String> userGlobalTable = builder.globalTable("user-table");
        KStream<String, String> userPurchases = builder.stream("user-purchases");

        KStream<String, String> userPurchasesEnrichedJoin =
                userPurchases.join(
                        userGlobalTable,
                        /* map from the (key, value) of this stream to the key of the GlobalKTable */
                        (key, value) -> key,
                        (userPurchase, userInfo) -> "Purchase = " + userPurchase + ", UserInfo=[" + userInfo + "]"
                );

        userPurchasesEnrichedJoin.to("user-purchases-enriched-inner-join");

        // ---------------

        KStream<String, String> userPurchasesEnrichedLeftJoin = userPurchases.leftJoin(
                userGlobalTable,
                (key, value) -> key,
                (userPurchase, userInfo) -> {
                    // as this is a left join we need to check if userInfo is null
                    if(userInfo != null) {
                        return "Purchase = " + userPurchase + ", UserInfo=[ " + userInfo + "]";
                    } else {
                        return "Purchase = " + userPurchase + ", UserInfo=[NULL]";
                    }
                }
        );

        userPurchasesEnrichedLeftJoin.to("user-purchases-enriched-left-join");

        KafkaStreams streams = new KafkaStreams(builder.build(), properties);
        streams.cleanUp(); // <-- do this only in DEV
        streams.start();

        System.out.println(streams.toString());

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

    }
}
