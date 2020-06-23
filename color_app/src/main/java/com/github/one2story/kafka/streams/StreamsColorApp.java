package com.github.one2story.kafka.streams;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Arrays;
import java.util.Properties;
import java.util.PropertyPermission;

public class StreamsColorApp {
    public static void main(String[] args) {

        Properties properties = new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "user-colors");
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        // Exactly once - idempotence of streams operations
        properties.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE);

        // Disabled cache to to demonstrate all the steps involved in the transformation - DO NOT USE IN PROD
        properties.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, "0");

        StreamsColorApp colorApp = new StreamsColorApp();
        colorApp.trueSolution(properties);

    }

    public void trueSolution(Properties properties)
    {
        /*          T O P O L O G Y
            1. Read one topic from Kafka (KStream)
            2. Filter bad values
            3. SelectKey that will be the user id
            4. MapValues to extract the color (as lowercase)
            5. Filter to remove bad colors
            6. WRITE to Kafka as an intermediary topic

            7. READ from Kafka as KTable
            8. GroupBy colors
            9. Count to count colors occurrences (KTable)
            10. WRITE to Kafka as final topic.

         */

        String splitter = ":";

        StreamsBuilder streamsBuilder = new StreamsBuilder();
        KStream<String, String> colorRegistratorInput = streamsBuilder.stream("user-colors-input");
        KStream<String, String> colorRegistrator = colorRegistratorInput.filter((key, value) -> value.contains(splitter))
                .selectKey((key, value) -> value.split(splitter)[0].toLowerCase())
                .mapValues(value -> value.split(splitter)[1].toLowerCase())
                .filter((user, color) -> Arrays.asList("green", "blue", "red").contains(color));

        colorRegistrator.to("user-keys-and-colors");


        KTable<String, String> usersAndColorsTable = streamsBuilder.table("user-keys-and-colors");
        KTable<String, Long> currentColors = usersAndColorsTable
                .groupBy((key, color) -> new KeyValue<>(color,color))
                .count(Named.as("CountsByColors"));

        currentColors.toStream().to("user-colors-output", Produced.with(Serdes.String(), Serdes.Long()));

        KafkaStreams streams = new KafkaStreams(streamsBuilder.build(), properties);
        streams.start();

        System.out.println(streams.toString());

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }

    public void mySolution(Properties properties)
    {
        String splitter = ":";

        StreamsBuilder streamsBuilder = new StreamsBuilder();
        KStream<String, String> colorRegistratorInput = streamsBuilder.stream("user-colors-input");
        // to UPPER case  -> split by ":" and set the key  ->  split by ":" and set the value
        KTable<String, String> colorRegistrator = colorRegistratorInput.filter((key, values) -> values.contains(splitter))
                .mapValues(value -> value.toUpperCase())
                .selectKey((_key, value) -> value.split(splitter)[0])
                .mapValues(value -> value.split(splitter)[1])
                .toTable();

        colorRegistrator.toStream().to("user-colors-output", Produced.with(Serdes.String(), Serdes.String()));

        KafkaStreams streams = new KafkaStreams(streamsBuilder.build(), properties);
        streams.start();

        System.out.println(streams.toString());
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }

}
