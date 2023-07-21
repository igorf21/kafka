package org.apache.kafka.streams.examples.foreignKeyJoin;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.streams.examples.avro.TrackPurchase;
//import org.apache.kafka.streams.examples.

import java.util.Properties;

public class PurchaceProducer {
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
        Producer<Long, TrackPurchase> producer = new KafkaProducer<Long, TrackPurchase>(props);
        TrackPurchase tackPurchase = new TrackPurchase();


        tackPurchase.setId(100);
        tackPurchase.setAlbumId(5);
        tackPurchase.setSongTitle("Houses Of The Holy");
        tackPurchase.setPrice(0.99);
        ProducerRecord<String, TrackPurchase> record =
                new ProducerRecord<String, TrackPurchase>("purchases","100",tackPurchase);
        try {
            producer.send(new ProducerRecord<>("purchases",100L,tackPurchase)).get();
        } catch (Exception e) {
            e.printStackTrace();
        }

        tackPurchase.setId(101);
        tackPurchase.setAlbumId(8);
        tackPurchase.setSongTitle("King Of Rock");
        tackPurchase.setPrice(0.99);

        try {
            producer.send(new ProducerRecord<>("purchases",101L,tackPurchase)).get();
        } catch (Exception e) {
            e.printStackTrace();
        }

        tackPurchase.setId(102);
        tackPurchase.setAlbumId(6);
        tackPurchase.setSongTitle("Shot Down In Flames");
        tackPurchase.setPrice(0.99);

        try {
            producer.send(new ProducerRecord<>("purchases",102L,tackPurchase)).get();
        } catch (Exception e) {
            e.printStackTrace();
        }

        tackPurchase.setId(103);
        tackPurchase.setAlbumId(7);
        tackPurchase.setSongTitle("Rock The Bells");
        tackPurchase.setPrice(0.99);

        try {
            producer.send(new ProducerRecord<>("purchases",103L,tackPurchase)).get();
        } catch (Exception e) {
            e.printStackTrace();
        }

        tackPurchase.setId(104);
        tackPurchase.setAlbumId(8);
        tackPurchase.setSongTitle("Can You Rock It Like This");
        tackPurchase.setPrice(0.99);

        try {
            producer.send(new ProducerRecord<>("purchases",104L,tackPurchase)).get();
        } catch (Exception e) {
            e.printStackTrace();
        }

        tackPurchase.setId(105);
        tackPurchase.setAlbumId(6);
        tackPurchase.setSongTitle("Hiway To Hell");
        tackPurchase.setPrice(0.99);

        try {
            producer.send(new ProducerRecord<>("purchases",105L,tackPurchase)).get();
        } catch (Exception e) {
            e.printStackTrace();
        }

        tackPurchase.setId(106);
        tackPurchase.setAlbumId(5);
        tackPurchase.setSongTitle("Kshmir");
        tackPurchase.setPrice(0.99);

        try {
            producer.send(new ProducerRecord<>("purchases",106L,tackPurchase)).get();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }


}
