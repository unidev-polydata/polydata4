package com.unidev.polydata4.sqlite;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.unidev.polydata4.api.AbstractPolydata;
import com.unidev.polydata4.domain.BasicPoly;
import com.unidev.polydata4.domain.BasicPolyList;
import com.unidev.polydata4.domain.InsertRequest;
import com.unidev.polydata4.domain.PolyQuery;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.flywaydb.core.Flyway;
import org.sqlite.SQLiteDataSource;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Polydata storage backed by SQLite databases.
 */
@RequiredArgsConstructor
@Slf4j
public class PolydataSqlite extends AbstractPolydata {

    private static final String DB_FILE_EXTENSION = ".db.sqlite";

    private final File rootDir;

    private final ObjectMapper objectMapper;


    private final Map<String, SQLiteDataSource> connections = new ConcurrentHashMap<>();

    @Override
    public void prepareStorage() {

    }

    @Override
    public BasicPoly create(String poly) {
        SQLiteDataSource datasource = fetchDataSource(poly);
        try {
            Flyway flyway = Flyway.configure()
                    .dataSource(datasource.getUrl(), "", "")
                    .locations("polydata-sqlite")
                    .outOfOrder(true)
                    .load();
            flyway.migrate();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        BasicPoly config = new BasicPoly();
        config._id(CONFIG_KEY);
        config.put(ITEM_PER_PAGE, DEFAULT_ITEM_PER_PAGE);
        config(poly, config);
        metadata(poly, BasicPoly.newPoly(METADATA_KEY));

        return config(poly).get();

    }

    @Override
    public boolean exists(String poly) {
        return getDbFile(poly).exists();
    }

    @Override
    public Optional<BasicPoly> config(String poly) {
        return readInternal(poly, CONFIG_KEY);
    }

    @Override
    public void config(String poly, BasicPoly config) {
        persistInternal(poly, config);
    }

    @Override
    public Optional<BasicPoly> metadata(String poly) {
        return readInternal(poly, METADATA_KEY);
    }

    @Override
    public void metadata(String poly, BasicPoly metadata) {
        persistInternal(poly, metadata);
    }

    @Override
    public Optional<BasicPoly> index(String poly) {
        return Optional.empty();
    }

    @Override
    public Optional<BasicPoly> indexData(String poly, String indexId) {
        return Optional.empty();
    }

    @Override
    public BasicPolyList insert(String poly, Collection<InsertRequest> insertRequests) {
        BasicPolyList result = new BasicPolyList();

        Set<String> ids = new HashSet<>();
        insertRequests.forEach(persistRequest -> ids.add(persistRequest.getPoly()._id()));
        BasicPolyList existingPolys = read(poly, ids);

        Collection<InsertRequest> toInsert = new HashSet<>();
        Collection<InsertRequest> toUpdate = new HashSet<>();

        insertRequests.forEach(persistRequest -> {
            BasicPoly polyToPersist = persistRequest.getPoly();
            if (existingPolys.hasPoly(polyToPersist._id())) {
                toUpdate.add(persistRequest);
            } else {
                toInsert.add(persistRequest);
            }
        });

        try(Connection connection = fetchConnection(poly)) {
            for (InsertRequest request : toInsert) {
                String id = request.getPoly()._id();

                Set<String> indexToPersist = request.getIndexToPersist();
                String tagString = "";

                for (String index : indexToPersist) {
                    tagString += "|" + index + "|";
                }

                try {
                    BasicPoly data = request.getPoly();
                    String jsonData = objectMapper.writeValueAsString(data);

                    long createDate = Long.parseLong(
                            data.fetch("_create_date", System.currentTimeMillis()) + "");
                    long updateDate = Long.parseLong(
                            data.fetch("_update_date", System.currentTimeMillis()) + "");

                    PreparedStatement preparedStatement = connection
                            .prepareStatement(
                                    "INSERT INTO data(_id, data, tags, create_date, update_date) VALUES(?, ?, ?, ?, ?)");

                    preparedStatement.setString(1, id);
                    preparedStatement.setString(2, jsonData);
                    preparedStatement.setString(3, tagString);
                    preparedStatement.setLong(4, createDate);
                    preparedStatement.setLong(5, updateDate);
                    preparedStatement.execute();
                    result.add(request.getPoly());
                    preparedStatement.close();

                    // tag index increment
                    for (String tag : indexToPersist) {
                        PreparedStatement tagIndexUpdate = connection.prepareStatement(
                                "    INSERT OR IGNORE INTO tags_index(poly, tag, tag_count) VALUES (?, ?, 0);"
                        );
                        tagIndexUpdate.setString(1, name);
                        tagIndexUpdate.setString(2, tag);
                        tagIndexUpdate.execute();

                        tagIndexUpdate = connection.prepareStatement(
                                "UPDATE tags_index SET tag_count = tag_count + 1 WHERE poly = ? AND tag = ?;"
                        );
                        tagIndexUpdate.setString(1, name);
                        tagIndexUpdate.setString(2, tag);
                        tagIndexUpdate.execute();
                        tagIndexUpdate.close();
                    }
                } catch (Exception throwables) {
                    log.error("Failed to persist poly", throwables);
                    throw new RuntimeException(throwables);
                }
            }

        } catch (Exception e) {
            throw new RuntimeException(e);
        }


        if (!toUpdate.isEmpty()) {
            update(poly, toUpdate);
        }

        return result;
    }

    @Override
    public BasicPolyList update(String poly, Collection<InsertRequest> insertRequests) {
        BasicPolyList result = new BasicPolyList();
        try(Connection connection = fetchConnection(poly)) {
            connection.setAutoCommit(false);
            PreparedStatement preparedStatement = connection
                    .prepareStatement("UPDATE data SET data=?, tags=?, update_date=? WHERE _id=?");
            for (InsertRequest request : insertRequests) {
                Set<String> indexToPersist = request.getIndexToPersist();
                String tagString = "";

                for (String index : indexToPersist) {
                    tagString += "|" + index + "|";
                }
                BasicPoly data = request.getPoly();
                String id = data._id();
                String jsonData = objectMapper.writeValueAsString(data);
                preparedStatement.setString(1, jsonData);
                preparedStatement.setString(2, tagString);
                preparedStatement.setLong(3, System.currentTimeMillis());
                preparedStatement.setString(4, id);
                preparedStatement.addBatch();

                result.add(data);
            }
            preparedStatement.executeBatch();
            connection.commit();
            connection.setAutoCommit(true);
            preparedStatement.close();
        } catch (Exception e) {
            log.error("Failed to persist poly", e);
            throw new RuntimeException(e);
        }

        return result;
    }

    @Override
    public BasicPolyList read(String poly, Set<String> ids) {
        return null;
    }

    @Override
    public BasicPolyList remove(String poly, Set<String> ids) {
        return null;
    }

    @Override
    public BasicPolyList query(String poly, PolyQuery polyQuery) {
        return null;
    }

    @Override
    public Long count(String poly, PolyQuery polyQuery) {
        return null;
    }

    @Override
    public BasicPolyList list() {
        BasicPolyList list = new BasicPolyList();
        File[] files = rootDir.listFiles();
        if (files != null) {
            for(File file : files) {
                if (file.getName().endsWith(DB_FILE_EXTENSION)) {
                    String poly = file.getName().replace(DB_FILE_EXTENSION, "");
                    list.add(BasicPoly.newPoly(poly));
                }
            }
        }
        return list;
    }

    @Override
    public void open() {

    }

    @Override
    public void close() throws IOException {

    }

    private void persistInternal(String poly, BasicPoly data) {
        try(Connection connection = fetchConnection(poly)) {
            PreparedStatement preparedStatement = connection
                    .prepareStatement("INSERT OR REPLACE INTO internal(_id, data, create_date, update_date) VALUES(?,?,?,?);");
            preparedStatement.setString(1, data._id());
            preparedStatement.setString(2, objectMapper.writeValueAsString(data));
            preparedStatement.setLong(3, System.currentTimeMillis());
            preparedStatement.setLong(4, System.currentTimeMillis());
            preparedStatement.executeUpdate();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private Optional<BasicPoly> readInternal(String poly, String id) {
        try(Connection connection = fetchConnection(poly)) {
            PreparedStatement preparedStatement = connection
                    .prepareStatement("SELECT data FROM internal WHERE _id=?");
            preparedStatement.setString(1, id);

            ResultSet resultSet = preparedStatement.executeQuery();
            if (resultSet.next()) {
                String rawData = resultSet.getString("data");
                return Optional.ofNullable(objectMapper.readValue(rawData, BasicPoly.class));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return Optional.empty();
    }

    private SQLiteDataSource fetchDataSource(String poly) {
        return connections.computeIfAbsent(poly, k -> {
            File dbFile = getDbFile(k);
            SQLiteDataSource sqLiteDataSource = new SQLiteDataSource();
            sqLiteDataSource.setUrl("jdbc:sqlite:" + dbFile.getAbsolutePath());
            return sqLiteDataSource;
        });
    }



    private File getDbFile(String poly) {
        File dbFile = new File(rootDir, poly + DB_FILE_EXTENSION);
        return dbFile;
    }

    private Connection fetchConnection(String poly) {
        try {
            return fetchDataSource(poly).getConnection();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    static {
        try {
            Class.forName("org.sqlite.JDBC");
        } catch (ClassNotFoundException e) {
            log.error("Failed to load SQLite driver", e);
        }
    }
}
