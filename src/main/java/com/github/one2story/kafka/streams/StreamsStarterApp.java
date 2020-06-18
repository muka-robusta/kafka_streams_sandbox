package com.github.one2story.kafka.streams;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Arrays;
import java.util.Properties;

public class StreamsStarterApp {

    public static void main(String[] args) {
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "word-count");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());


        StreamsBuilder builder = new StreamsBuilder();
        // 1 - Stream from Kafka
        KStream<String, String> wordCountInput = builder.stream("word-count-input");

                // 2 - Map values to lowercase
        KTable<String, Long> wordCounts = wordCountInput.mapValues(value -> value.toLowerCase())
                // 3 - Flatmap values split by space
                .flatMapValues(value -> Arrays.asList(value.split(" ")))
                // 4 - Select key to to apply a key(discard the old key)
                .selectKey((ignoredKey, word) -> word)
                // 5 - group by key before aggregation
                .groupByKey()
                // 6 - count occurences
                .count(Named.as("Counts"));

        // 7 - write the results back to kafka
        wordCounts.toStream().to("word-count-output", Produced.with(Serdes.String(), Serdes.Long()));

        KafkaStreams streams = new KafkaStreams(builder.build(), config);
        streams.start();

        // Printed the topology
        System.out.println(streams.toString());

        // Add shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }

}
