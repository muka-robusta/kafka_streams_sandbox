package com.github.one2story.kafka.streams;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Instant;
import java.util.Properties;
import java.util.concurrent.ThreadLocalRandom;

public class BankTransactionsProducer {
    public static void main(String[] args) {
        Properties properties = new Properties();

        // Kafka bootstrap server
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // Producer acks
        properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        properties.setProperty(ProducerConfig.RETRIES_CONFIG, "3");
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "1");

        // leverage idempotent producer
        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true"); // Ensure we dont push duplicates

        Producer<String, String> producer = new KafkaProducer<String, String>(properties);

        int i = 0;
        while(true) {
            System.out.println("Producing batch: " + i);
            try {
                producer.send(newRandomTransaction("ilya"));
                Thread.sleep(100);
                producer.send(newRandomTransaction("Ivan"));
                Thread.sleep(100);
                producer.send(newRandomTransaction("Dmytro"));
                Thread.sleep(100);
                i++;
            } catch (InterruptedException ex) {
                break;
            }
        }
        producer.close();
    }

    public static ProducerRecord<String, String> newRandomTransaction(String name){
        ObjectNode transaction = JsonNodeFactory.instance.objectNode();
        Integer amount = ThreadLocalRandom.current().nextInt(0, 100);
        Instant now = Instant.now();

        transaction.put("name", name);
        transaction.put("amount", amount);
        transaction.put("time", now.toString());

        return new ProducerRecord<>("bank-transactions", name, transaction.toString());
    }

}
