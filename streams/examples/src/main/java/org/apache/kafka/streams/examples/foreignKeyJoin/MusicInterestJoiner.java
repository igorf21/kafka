package org.apache.kafka.streams.examples.foreignKeyJoin;

import org.apache.kafka.streams.examples.avro.Album;
import org.apache.kafka.streams.examples.avro.MusicInterest;
import org.apache.kafka.streams.examples.avro.TrackPurchase;
import org.apache.kafka.streams.kstream.ValueJoiner;

public class MusicInterestJoiner implements ValueJoiner<Album, TrackPurchase, MusicInterest> {
    public MusicInterest apply(Album album, TrackPurchase trackPurchase) {
        return MusicInterest.newBuilder()
                .setId(album.getId() + "-" + trackPurchase.getId())
                .setGenre(album.getGenre())
                .setArtist(album.getArtist())
                .build();
    }
}

