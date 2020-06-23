package com.github.one2story.kafka.streams;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class KafkaProducerTest {
    @Test
    public void newRandomTransactionsTest(){
        ProducerRecord<String, String> record = BankTransactionsProducer.newRandomTransaction("Ilya");
        String key = record.key();
        String value = record.value();

        assertEquals(key, "Ilya");

        ObjectMapper mapper = new ObjectMapper();
        try {
            JsonNode node = mapper.readTree(value);
            assertEquals(node.get("name").asText(), "Ilya");
            assertTrue("Amount shuld be less than 100", node.get("amount").asInt() < 100);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }

    }
}
