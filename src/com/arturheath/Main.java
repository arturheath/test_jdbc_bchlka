package com.arturheath;

import com.arturheath.model.Artist;
import com.arturheath.model.Datasourse;
import com.arturheath.model.SongArtist;

import java.sql.Statement;
import java.util.*;

public class Main {

    public static void main(String[] args) {
        Datasourse datasourse = new Datasourse();
        if (!datasourse.open()){
            System.out.println("Can't open datasourse");
            return;
        }

        List<Artist> artists = datasourse.queryArtists(Datasourse.ORDER_BY_ASC);
        if (artists == null){
            System.out.println("No artists!");
            return;
        }

        for (Artist artist : artists){
            System.out.println("ID = " + artist.getId() + ", Name = " + artist.getName());
        }

        System.out.println("===============================================================");

        List<String> albumsOfPinkFloyd = datasourse.queryAlbumsForArtists("Pink Floyd", Datasourse.ORDER_BY_DESC);
        if (albumsOfPinkFloyd == null){
            System.out.println("No albums");
        }
        for (String album : albumsOfPinkFloyd){
            System.out.println(album);
        }

        System.out.println("===============================================================");

        List<SongArtist> songArtists = datasourse.queryArtistForSong("Hey You", Datasourse.ORDER_BY_ASC);
        if (songArtists == null){
            System.out.println("No artists");
            return;
        }
        for (SongArtist songArtist : songArtists){
            System.out.println("Artist = " + songArtist.getArtistName() + " Album = " + songArtist.getAlbumName() + " Track = " + songArtist.getTrack());
        }

        System.out.println("===============================================================");

        datasourse.querySongsMetaData();

        System.out.println("===============================================================");

        int count = datasourse.getCount(Datasourse.TABLE_SONGS);
        System.out.println("Number of songs = " + count);

        //datasourse.dropTheView(Datasourse.TABLE_ARTIST_SONG_VIEW);

        datasourse.createViewForSongArtists();

//
//        Scanner scanner = new Scanner(System.in);
//        String title = scanner.nextLine();
//        songArtists = datasourse.querySongInfoView(title);
//        if (songArtists == null){
//            System.out.println("Couldn't find any artists for the song");
//            return;
//        }
//
//        for (SongArtist songArtist : songArtists){
//            System.out.println("Artists = " + songArtist.getArtistName() + " Album = " + songArtist.getAlbumName() + " Track # = " + songArtist.getTrack());
//        }

        System.out.println("=========================================================================");

        datasourse.insertSong("Shadow Play", "Joy Division", "Unknown Pleasures", 7);


        datasourse.close();
    }
}
