package com.arturheath.model;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class Datasourse {

    public static final String DB_NAME = "music.db";
    public static final String CONNECTION_STRING = "jdbc:sqlite:C:\\Users\\arsha\\IdeaProjects\\MusicDB\\" + DB_NAME;

    public static final String TABLE_ALBUMS = "albums";
    public static final String COLUMN_ALBUM_ID = "_id";
    public static final String COLUMN_ALBUM_NAME = "name";
    public static final String COLUMN_ALBUM_ARTIST = "artist";
    public static final int INDEX_ALBUM_ID = 1;
    public static final int INDEX_ALBUM_NAME = 2;
    public static final int INDEX_ALBUM_ARTIST = 3;

    public static final String TABLE_ARTISTS = "artists";
    public static final String COLUMN_ARTIST_ID = "_id";
    public static final String COLUMN_ARTIST_NAME = "name";
    public static final int INDEX_ARTIST_ID = 1;
    public static final int INDEX_ARTIST_NAME = 2;

    public static final String TABLE_SONGS = "songs";
    public static final String COLUMN_SONG_ID = "_id";
    public static final String COLUMN_SONG_ALBUM = "album";
    public static final String COLUMN_SONG_TITLE = "title";
    public static final String COLUMN_SONG_TRACK = "track";
    public static final int INDEX_SONG_ID = 1;
    public static final int INDEX_SONG_ALBUM = 2;
    public static final int INDEX_SONG_TITLE = 3;
    public static final int INDEX_SONG_TRACK = 4;

    public static final int ORDER_BY_NONE = 1;
    public static final int ORDER_BY_ASC = 2;
    public static final int ORDER_BY_DESC = 3;

    public static final String QUERY_ALBUMS_BY_ARTIST_START =
            "SELECT " + TABLE_ALBUMS + "." + COLUMN_ALBUM_NAME + " FROM " + TABLE_ALBUMS
                    + " INNER JOIN " + TABLE_ARTISTS + " ON " + TABLE_ALBUMS + "." + COLUMN_ALBUM_ARTIST + " = " + TABLE_ARTISTS + "." + COLUMN_ARTIST_ID
                    + " WHERE " + TABLE_ARTISTS + "." + COLUMN_ARTIST_NAME + "=";
    public static final String QUERY_ALBUMS_BY_ARTIST_SORT = " ORDER BY " + TABLE_ALBUMS + "." + COLUMN_ALBUM_NAME + " COLLATE NOCASE ";

    public static final String QUERY_ARTISTS_START = "SELECT * FROM " + TABLE_ARTISTS;
    public static final String QUERY_ARTISTS_SORT = " ORDER BY " + COLUMN_ARTIST_NAME + " COLLATE NOCASE ";

    public static final String QUERY_ARTIST_FOR_SONG_START =
            "SELECT " + TABLE_ARTISTS + "." + COLUMN_ARTIST_NAME + ", " + TABLE_ALBUMS + "." + COLUMN_ALBUM_NAME + ", " + TABLE_SONGS + "." + COLUMN_SONG_TRACK
                    + " FROM " + TABLE_SONGS
                    + " INNER JOIN " + TABLE_ALBUMS + " ON " + TABLE_SONGS + "." + COLUMN_SONG_ALBUM + " = " + TABLE_ALBUMS + "." + COLUMN_ALBUM_ID
                    + " INNER JOIN " + TABLE_ARTISTS + " ON " + TABLE_ALBUMS + "." + COLUMN_ALBUM_ARTIST + " = " + TABLE_ARTISTS + "." + COLUMN_ARTIST_ID
                    + " WHERE " + TABLE_SONGS + "." + COLUMN_SONG_TITLE + " = ";
    public static final String QUERY_ARTIST_FOR_SONG_SORT =
            " ORDER BY " + TABLE_ARTISTS + "." + COLUMN_ARTIST_NAME + ", "
                    + TABLE_ALBUMS + "." + COLUMN_ALBUM_NAME
                    + " COLLATE NOCASE";

    public static final String TABLE_ARTIST_SONG_VIEW = "artist_list";
    public static final String CREATE_ARTIST_FOR_SONG_VIEW = "CREATE VIEW IF NOT EXISTS "
            + TABLE_ARTIST_SONG_VIEW + " AS SELECT " + TABLE_ARTISTS + "." + COLUMN_ARTIST_NAME + ", "
            + TABLE_ALBUMS + "." + COLUMN_ALBUM_NAME + " AS " + COLUMN_SONG_ALBUM + ", "
            + TABLE_SONGS + "." + COLUMN_SONG_TRACK + ", " + TABLE_SONGS + "." + COLUMN_SONG_TITLE
            + " FROM " + TABLE_SONGS
            + " INNER JOIN " + TABLE_ALBUMS + " ON " + TABLE_SONGS
            + "." + COLUMN_SONG_ALBUM + " = " + TABLE_ALBUMS + "." + COLUMN_ALBUM_ID
            + " INNER JOIN " + TABLE_ARTISTS + " ON " + TABLE_ALBUMS + "." + COLUMN_ALBUM_ARTIST
            + " = " + TABLE_ARTISTS + "." + COLUMN_ARTIST_ID
            + " ORDER BY "
            + TABLE_ARTISTS + "." + COLUMN_ARTIST_NAME + ", "
            + TABLE_ALBUMS + "." + COLUMN_ALBUM_NAME + ", "
            + TABLE_SONGS + "." + COLUMN_SONG_TRACK;

    public static final String QUERY_VIEW_SONG_INFO = "SELECT " + COLUMN_ARTIST_NAME + ", "
            + COLUMN_ALBUM_NAME + ", " + COLUMN_SONG_TRACK + " FROM " + TABLE_ARTIST_SONG_VIEW
            + " WHERE " + COLUMN_SONG_TITLE + " = ";

    public static final String QUERY_VIEW_SONG_INFO_PREP = "SELECT " + COLUMN_ARTIST_NAME + ", "
            + COLUMN_ALBUM_NAME + ", " + COLUMN_SONG_TRACK + " FROM " + TABLE_ARTIST_SONG_VIEW
            + " WHERE " + COLUMN_SONG_TITLE + " = ?";

    public static final String INSERT_ARTIST = "INSERT INTO " + TABLE_ARTISTS + "("
            + COLUMN_ARTIST_NAME + ") VALUES(?)";

    public static final String INSERT_ALBUMS = "INSERT INTO " + TABLE_ALBUMS + "("
            + COLUMN_ALBUM_NAME + ", " + COLUMN_ALBUM_ARTIST + ") VALUES (?, ?)";

    public static final String INSERT_SONG = "INSERT INTO " + TABLE_SONGS + "("
            + COLUMN_SONG_TRACK + ", " + COLUMN_SONG_TITLE + ", " + COLUMN_SONG_ALBUM
            + ") VALUES(?, ?, ?)";

    public static final String QUERY_ARTIST = "SELECT " + COLUMN_ARTIST_ID + " FROM " + TABLE_ARTISTS
            + " WHERE " + COLUMN_ARTIST_NAME + " = ?";

    public static final String QUERY_ALBUM = "SELECT " + COLUMN_ALBUM_ID + " FROM " + TABLE_ALBUMS
            + " WHERE " + COLUMN_ALBUM_NAME + " = ?";

    private Connection conn;

    private PreparedStatement querySongInfoView;
    private PreparedStatement insertIntoArtists;
    private PreparedStatement insertIntoAlbums;
    private PreparedStatement insertIntoSongs;
    private PreparedStatement queryArtist;
    private PreparedStatement queryAlbum;

    public boolean open() {
        try {
            conn = DriverManager.getConnection(CONNECTION_STRING);
            querySongInfoView = conn.prepareStatement(QUERY_VIEW_SONG_INFO_PREP);
            // RETURN_GENERATED_KEYS параметр нужен для извлечения ID, которые генерируются автоматически с помощью PRIMARY KEY
            insertIntoArtists = conn.prepareStatement(INSERT_ARTIST, Statement.RETURN_GENERATED_KEYS);
            insertIntoAlbums = conn.prepareStatement(INSERT_ALBUMS, Statement.RETURN_GENERATED_KEYS);
            insertIntoSongs = conn.prepareStatement(INSERT_SONG);
            queryArtist = conn.prepareStatement(QUERY_ARTIST);
            queryAlbum = conn.prepareStatement(QUERY_ALBUM);
            return true;
        } catch (SQLException e) {
            System.out.println("Couldn't connect to a database: " + e.getMessage());
            return false;
        }
    }

    public void close() {
        try {
            if (querySongInfoView != null) {
                querySongInfoView.close();
            }
            if (insertIntoArtists != null) {
                insertIntoArtists.close();
            }
            if (insertIntoAlbums != null) {
                insertIntoAlbums.close();
            }
            if (insertIntoSongs != null) {
                insertIntoSongs.close();
            }
            if (queryAlbum != null) {
                queryAlbum.close();
            }
            if (queryArtist != null) {
                queryArtist.close();
            }
            if (conn != null) {
                conn.close();
            }
        } catch (SQLException e) {
            System.out.println("Couldn't close connection " + e.getMessage());
        }
    }

    public List<SongArtist> queryArtistForSong(String song, int sortOrder) {
        StringBuilder sb = new StringBuilder(QUERY_ARTIST_FOR_SONG_START);
        sb.append("'");
        sb.append(song);
        sb.append("'");

        if (sortOrder != ORDER_BY_NONE) {
            sb.append(QUERY_ARTIST_FOR_SONG_SORT);
            if (sortOrder == ORDER_BY_ASC) {
                sb.append(" ASC ");
            } else {
                sb.append(" DESC ");
            }
        }
        System.out.println("SQL statement = " + sb.toString());

        List<SongArtist> artists = new ArrayList<>();

        try (Statement statement = conn.createStatement();
             ResultSet result = statement.executeQuery(sb.toString())) {
            while (result.next()) {
                SongArtist songArtist = new SongArtist();
                songArtist.setArtistName(result.getString(1));
                songArtist.setAlbumName(result.getString(2));
                songArtist.setTrack(result.getInt(3));
                artists.add(songArtist);
            }
            return artists;
        } catch (SQLException e) {
            System.out.println("Query failed " + e.getMessage());
            return null;
        }
    }

    public void querySongsMetaData() {
        String sql = "SELECT * FROM " + TABLE_SONGS;

        try (Statement statement = conn.createStatement();
             ResultSet result = statement.executeQuery(sql)) {

            ResultSetMetaData meta = result.getMetaData();
            int numColomns = meta.getColumnCount();

            for (int i = 1; i <= numColomns; i++) {
                System.out.format("Column %d in the songs table is %s\n", i, meta.getColumnName(i));
            }
        } catch (SQLException e) {
            System.out.println("Query failed " + e.getMessage());
        }
    }


    public int getCount(String table) {
        String sql = "SELECT COUNT(*) AS count FROM " + table;

        try (Statement statement = conn.createStatement();
             ResultSet result = statement.executeQuery(sql)) {

            int count = result.getInt("count");
            return count;
        } catch (SQLException e) {
            System.out.println("Query failed " + e.getMessage());
            return -1;
        }
    }


    public boolean createViewForSongArtists() {
        try (Statement statement = conn.createStatement()) {
            statement.execute(CREATE_ARTIST_FOR_SONG_VIEW);
            return true;
        } catch (SQLException e) {
            System.out.println("Creating a view failed " + e.getMessage());
            return false;
        }
    }


    public boolean dropTheView(String view) {
        String sql = "DROP VIEW " + view;

        try (Statement statement = conn.createStatement()) {
            statement.execute(sql);
            return true;
        } catch (SQLException e) {
            System.out.println("Dropping a view failed " + e.getMessage());
            return false;
        }
    }


    public List<SongArtist> querySongInfoView(String title) {
        try {
            //setString потому что параметр типа стринг,
            // 1 - это который по счету заполнитель (?) в sql выражении мы заменяем (может быть несколько)

            // экземпляр PreparedStatement был создан при открытии соединения (Connection) ранее
            // вроде как в целяъ улучшения производительности
            querySongInfoView.setString(1, title);
            ResultSet result = querySongInfoView.executeQuery();

            List<SongArtist> songArtists = new ArrayList<>();
            while (result.next()) {
                SongArtist songArtist = new SongArtist();
                songArtist.setArtistName(result.getString(1));
                songArtist.setAlbumName(result.getString(2));
                songArtist.setTrack(result.getInt(3));
                songArtists.add(songArtist);
            }
            return songArtists;
        } catch (SQLException e) {
            System.out.println("Query failed " + e.getMessage());
            return null;
        }
    }


    public List<Artist> queryArtists(int sortOrder) {
        StringBuilder sb = new StringBuilder(QUERY_ARTISTS_START);

        if (sortOrder != ORDER_BY_NONE) {
            sb.append(QUERY_ARTISTS_SORT);
            if (sortOrder == ORDER_BY_ASC) {
                sb.append("ASC");
            } else {
                sb.append("DESC");
            }
        }
        System.out.println("SQL statement = " + sb.toString());

        try (Statement statement = conn.createStatement();
             ResultSet result = statement.executeQuery(sb.toString())) {

            List<Artist> artists = new ArrayList<>();

            while (result.next()) {
                Artist artist = new Artist();
                artist.setId(result.getInt(INDEX_ARTIST_ID));
                artist.setName(result.getString(INDEX_ARTIST_NAME));
                artists.add(artist);
            }
            return artists;
        } catch (SQLException e) {
            System.out.println("Query failed " + e.getMessage());
            return null;
        }
    }


    public List<String> queryAlbumsForArtists(String artist, int sortOrder) {
        StringBuilder sb = new StringBuilder(QUERY_ALBUMS_BY_ARTIST_START);
        sb.append("'");
        sb.append(artist);
        sb.append("'");

        if (sortOrder != ORDER_BY_NONE) {
            sb.append(QUERY_ALBUMS_BY_ARTIST_SORT);
            if (sortOrder == ORDER_BY_ASC) {
                sb.append("ASC");
            } else {
                sb.append("DESC");
            }
        }
        System.out.println("SQL statement = " + sb.toString());

        try (Statement statement = conn.createStatement();
             ResultSet result = statement.executeQuery(sb.toString())) {

            List<String> albums = new ArrayList<>();
            while (result.next()) {
                albums.add(result.getString(1));
            }
            return albums;
        } catch (SQLException e) {
            System.out.println("Query failed " + e.getMessage());
            return null;
        }
    }

    private int insertArtist(String name) throws SQLException {
        // дополняем SQL выраж именем артиста для поиска. 1 - номер заполнителя ("?")
        queryArtist.setString(1, name);
        // выполняем SQL выраж и сохр результаты в сет
        ResultSet result = queryArtist.executeQuery();
        // если сет что-то сохранил, значит артист уже есть в бд
        if (result.next()) {
            // тогда вернуть его айди
            return result.getInt(1);
            // иначе добавляем артиста
        } else {
            insertIntoArtists.setString(1, name);
            // сохраняет число рядов ,которые были затронуты SQL выраж (но мы добавляем только 1 ряд, поэтому ждем знач 1)
            int affectedRows = insertIntoArtists.executeUpdate();
            // если число рядов, которые мы изменили, не равно 1, что-то пошло не так
            if (affectedRows != 1) {
                throw new SQLException("Couldn't insert artist");
            }

            // сохраняем значение _id, кот будет присвоено новой записи автоматически (потому что primary key)
            ResultSet generatedKeys = insertIntoArtists.getGeneratedKeys();
            if (generatedKeys.next()) {
                return generatedKeys.getInt(1);
            } else {
                throw new SQLException("Couldn't get _id for artist");
            }
        }
    }


    private int insertAlbum(String albumName, int artistId) throws SQLException {
        queryAlbum.setString(1, albumName);
        ResultSet id = queryAlbum.executeQuery();

        if (id.next()) {
            return id.getInt(1);
        } else {
            insertIntoAlbums.setString(1, albumName);
            insertIntoAlbums.setInt(2, artistId);
            int affectedRows = insertIntoAlbums.executeUpdate();

            if (affectedRows != 1) {
                throw new SQLException("Couldn't insert an album");
            }

            ResultSet generatedKeys = queryAlbum.getGeneratedKeys();
            if (generatedKeys.next()) {
                return generatedKeys.getInt(1);
            } else {
                throw new SQLException("Couldn't get _id for album");
            }
        }
    }

    public void insertSong(String title, String artist, String album, int track) {

        try {
            // отключить автокомит
            conn.setAutoCommit(false);

            // этот метод вернет id артиста, не важно, был ли он уже в бд, или еще нет
            int artistId = insertArtist(artist);
            // то же самое для альбома
            int albumId = insertAlbum(album, artistId);
            // дополняем SQL код в PreparedStatement недостающими данными для дальнейшего применения
            insertIntoSongs.setInt(1, track);
            insertIntoSongs.setString(2, title);
            insertIntoSongs.setInt(8, albumId);

            // применяем SQL выражение и сохраняем значение количества рядов, в которые произошла запись (ожидаем только 1)
            int affectedRows = insertIntoSongs.executeUpdate();
            // проверяем предыдущий пункт, если 1 - ок, если 0 - не получилось, если > 1 - хз, но ниче хорошего
            if (affectedRows == 1) {
                // если все ок, то комитим
                conn.commit();
                // выбрас искл, если не все ок
            } else {
                throw new SQLException("Song insert failed");
            }
            // ловим искл и производим откат ( действие всего кода в этом методе можно отменить, так как был отключен автокомит)
            // исклю общего типа, т.к откат нужен при возникновении проблем любого рода
        } catch (Exception e) {
            System.out.println("Insert song exception " + e.getMessage());
            try {
                System.out.println("Performing rollback");
                conn.rollback();
            } catch (SQLException e2) {
                System.out.println("Oh boy! Things are really bad! " + e2.getMessage());
            }
        } finally {
            try {
                System.out.println("Resetting default commit behaviour");
                conn.setAutoCommit(true);
            } catch (SQLException e) {
                System.out.println("Couldn't reset auto-commit " + e.getMessage());
            }
        }
    }


}
