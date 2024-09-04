package com.mensageria;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.Scanner;

public class MensageriaProducer {
    public static void main(String[] args) {
        String topicName = "aula-kafka";

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);
        String value;
        Scanner sc = new Scanner(System.in);
        do {
            value = sc.nextLine();
            ProducerRecord<String, String> record = new ProducerRecord<>(topicName, value);
            producer.send(record);
            System.err.println("Enviado ====> " + value);
        } while (value != "0");

        producer.close();
    }
}
