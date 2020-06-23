package com.github.one2story.kafka.streams;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.one2story.kafka.streams.entities.Transaction;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Properties;

public class BankBalance {
    public static void main(String[] args) {

        String bootstrapServer = "localhost:9092";

        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

    }

    private void mySolution(Properties properties)
    {
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);
        int count = 1000;
        double maxAmount = 5000;
        String names[] = {"Ilya", "Max", "Stephane", "Ivan", "Dmytro"};
        String topicIn = "user-transactions";

        for(int i = 0; i < count; i++)
        {
            Integer amount = (int) (Math.random() * maxAmount);
            String currentName = names[((int)(Math.random() * names.length))];
            String transactionTime = Instant.now().toString();

            String kafkaStreamsValue = "{\"Name\": \"" + currentName + "\", \"amount\": "
                    + amount.toString() + ", \"time\": \"" + transactionTime + "\"}";

            ProducerRecord<String, String> record = new ProducerRecord<String, String>(topicIn, kafkaStreamsValue);
            producer.send(record);

            if(i % 1000 == 0)
            {
                try {
                    Thread.sleep(1000);
                }catch(InterruptedException ex)
                {
                    ex.printStackTrace();
                }
            }

        }

        producer.flush();
        producer.close();

        Double amount = 0.0;

        Properties streamProperties = getKStreamsProperties();
        StreamsBuilder streamsBuilder = new StreamsBuilder();
        KStream<String, String> accountStream = streamsBuilder.stream(topicIn);
        KTable<String, Long> accountTable = accountStream.selectKey((key, value) -> extractUsernameFromValue(value)).groupByKey().count();

    }

    private String extractUsernameFromValue(String value) {
        ObjectMapper mapper = new ObjectMapper();
        Transaction currentTransaction = null;
        try {
            currentTransaction = mapper.readValue(value, Transaction.class);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }

        if(currentTransaction == null){
            System.out.println("Unable to read ");
            return "";
        }else
        {
            return currentTransaction.getName();
        }
    }

    public Properties getKStreamsProperties()
    {
        String bootstrapServerStr = "localhost:9092";
        String applicationId = "bank-account";
        String autoOffsetReset = "earliest";

        Properties streamsProperties = new Properties();
        streamsProperties.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
        streamsProperties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServerStr);
        streamsProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, autoOffsetReset);
        streamsProperties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        streamsProperties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.Double().getClass());

        return streamsProperties;
    }
}
