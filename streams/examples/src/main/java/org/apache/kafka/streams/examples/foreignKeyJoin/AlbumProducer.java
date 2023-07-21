package org.apache.kafka.streams.examples.foreignKeyJoin;

import org.apache.kafka.streams.examples.avro.Album;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class AlbumProducer {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "kafka:9092");
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "serializers.custom.FlightSerializer");
        props.put("schema.registry.url", "http://kafka:8081");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,io.confluent.kafka.serializers.KafkaAvroSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,io.confluent.kafka.serializers.KafkaAvroSerializer.class);
        //Producer<String, Album> producer = new KafkaProducer<String, Album>(props);
        Producer<Long, Album> producer = new KafkaProducer<Long, Album>(props);
        Album album = new Album();


        album.setId(5);
        album.setArtist("Led Zeppelin");
        album.setGenre("Rock");
        album.setTitle("Physical Graffiti");

        try {
            producer.send(new ProducerRecord<>("albums",5L,album)).get();
        } catch (Exception e) {
            e.printStackTrace();
        }

        album.setId(6);
        album.setArtist("AC/DC");
        album.setGenre("Rock");
        album.setTitle("Highway To Hell");

        try {
            producer.send(new ProducerRecord<>("albums",6L,album)).get();
        } catch (Exception e) {
            e.printStackTrace();
        }

        album.setId(7);
        album.setArtist("LL Cool J");
        album.setGenre("Hip-Hop");
        album.setTitle("Radio");

        try {
            producer.send(new ProducerRecord<>("albums",7L,album)).get();
        } catch (Exception e) {
            e.printStackTrace();
        }

        album.setId(8);
        album.setArtist("Run-D.M.C");
        album.setGenre("Hip hop");
        album.setTitle("King of Rock");

        try {
            producer.send(new ProducerRecord<>("albums",8L,album)).get();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
