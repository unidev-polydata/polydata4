package com.unidev.polydata4.sqlite;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.unidev.polydata4.api.AbstractPolydata;
import com.unidev.polydata4.domain.*;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
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

    private static final String INDEX_KEY = "index";

    static {
        try {
            Class.forName("org.sqlite.JDBC");
        } catch (ClassNotFoundException e) {
            log.error("Failed to load SQLite driver", e);
        }
    }

    private final File rootDir;
    private final ObjectMapper objectMapper;
    private final Map<String, SQLiteDataSource> connections = new ConcurrentHashMap<>();

    private static Set<String> buildTagIndex(InsertRequest request) {
        Set<String> indexToPersist = request.getIndexToPersist();
        if (CollectionUtils.isEmpty(indexToPersist)) {
            indexToPersist = new HashSet<>();
        } else {
            indexToPersist = new HashSet<>(indexToPersist);
        }
        indexToPersist.add(DATE_INDEX);
        return indexToPersist;
    }

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
        return readInternal(poly, INDEX_KEY);
    }

    @Override
    public Optional<BasicPoly> indexData(String poly, String indexId) {
        Optional<BasicPoly> index = index(poly);
        return index.map(basicPoly -> basicPoly.fetch(indexId));
    }

    @Override
    public BasicPolyList insert(String poly, Collection<InsertRequest> insertRequests) {
        BasicPolyList result = new BasicPolyList();

        Set<String> ids = new HashSet<>();
        insertRequests.forEach(persistRequest -> ids.add(persistRequest.getData()._id()));
        BasicPolyList existingPolys = read(poly, ids);

        Collection<InsertRequest> toInsert = new HashSet<>();
        Collection<InsertRequest> toUpdate = new HashSet<>();

        insertRequests.forEach(persistRequest -> {
            BasicPoly polyToPersist = persistRequest.getData();
            if (existingPolys.hasPoly(polyToPersist._id())) {
                toUpdate.add(persistRequest);
            } else {
                toInsert.add(persistRequest);
            }
        });

        try (Connection connection = fetchConnection(poly)) {
            connection.setAutoCommit(false);
            PreparedStatement preparedStatement = connection
                    .prepareStatement(
                            "INSERT INTO data(_id, data, polydata_index, create_date, update_date) VALUES(?, ?, ?, ?, ?)");

            for (InsertRequest request : toInsert) {
                String id = request.getData()._id();
                Set<String> tags = buildTagIndex(request);
                String tagString = buildTagIndexString(tags);
                BasicPoly data = request.getData();
                data.put(INDEXES, tags);
                try {
                    String jsonData = objectMapper.writeValueAsString(data);

                    long createDate = Long.parseLong(
                            data.fetch("_create_date", System.currentTimeMillis()) + "");
                    long updateDate = Long.parseLong(
                            data.fetch("_update_date", System.currentTimeMillis()) + "");


                    preparedStatement.setString(1, id);
                    preparedStatement.setString(2, jsonData);
                    preparedStatement.setString(3, tagString);
                    preparedStatement.setLong(4, createDate);
                    preparedStatement.setLong(5, updateDate);
                    preparedStatement.addBatch();

                    result.add(request.getData());

                } catch (Exception e) {
                    log.error("Failed to persist poly {}", data._id(), e);
                    throw new RuntimeException(e);
                }
            }

            preparedStatement.executeBatch();
            connection.commit();
            connection.setAutoCommit(true);
            preparedStatement.close();

        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        log.info("Added polys {} ", toInsert.size());

        recalculateIndex(poly);

        if (!toUpdate.isEmpty()) {
            update(poly, toUpdate);
        }

        return result;
    }

    @Override
    public BasicPolyList update(String poly, Collection<InsertRequest> updateRequests) {
        BasicPolyList result = new BasicPolyList();
        try (Connection connection = fetchConnection(poly)) {
            connection.setAutoCommit(false);
            PreparedStatement preparedStatement = connection
                    .prepareStatement("UPDATE data SET data=?, polydata_index=?, update_date=? WHERE _id=?");
            for (InsertRequest request : updateRequests) {
                Set<String> tags = buildTagIndex(request);
                String tagString = buildTagIndexString(tags);
                BasicPoly data = request.getData();
                data.put(INDEXES, tags);
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

        recalculateIndex(poly);

        log.info("Updated polys {} ", updateRequests.size());
        return result;
    }

    @Override
    public BasicPolyList read(String poly, Set<String> ids) {
        BasicPolyList basicPolyList = new BasicPolyList();
        try (Connection connection = fetchConnection(poly)) {
            String q = createQuestionMarks(ids);
            PreparedStatement preparedStatement = connection
                    .prepareStatement("SELECT data FROM data WHERE _id IN ( " + q + ") ; ");
            int i = 1;
            for (String id : ids) {
                preparedStatement.setString(i++, id);
            }
            ResultSet resultSet = preparedStatement.executeQuery();
            while (resultSet.next()) {
                String rawData = resultSet.getString("data");
                BasicPoly basicPoly = objectMapper.readValue(rawData, BasicPoly.class);
                basicPolyList.add(basicPoly);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        return basicPolyList;
    }

    @Override
    public BasicPolyList remove(String poly, Set<String> ids) {
        BasicPolyList basicPolyList = read(poly, ids);
        try (Connection connection = fetchConnection(poly)) {
            String q = createQuestionMarks(ids);
            PreparedStatement preparedStatement = connection
                    .prepareStatement("DELETE FROM data WHERE _id IN (" + q + ") ; ");
            int i = 1;
            for (String id : ids) {
                preparedStatement.setString(i++, id);
            }
            long removedRows = preparedStatement.executeUpdate();
            log.info("Removed {} rows", removedRows);
            recalculateIndex(poly);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return basicPolyList;
    }

    @Override
    public BasicPolyList query(String poly, PolyQuery polyQuery) {
        BasicPolyQuery query = (BasicPolyQuery) polyQuery;
        Optional<BasicPoly> configPoly = config(poly);

        if (configPoly.isEmpty()) {
            throw new RuntimeException("Poly " + poly + " is not configured");
        }

        BasicPolyList list = new BasicPolyList();

        String index = DATE_INDEX;
        String tag = query.index();
        if (!StringUtils.isBlank(tag)) {
            index = tag;
        }
        BasicPoly config = configPoly.get();
        final int page = query.page() < 0 ? 0 : query.page();
        Integer defaultItemPerPage = config.fetch(ITEM_PER_PAGE, DEFAULT_ITEM_PER_PAGE);
        Integer itemPerPage = query.getOptions().fetch(ITEM_PER_PAGE, defaultItemPerPage);
        try (Connection connection = fetchConnection(poly)) {
            PreparedStatement preparedStatement = null;
            if (query.queryType() == BasicPolyQuery.QueryFunction.RANDOM) {
                int randomCount = query.option(RANDOM_COUNT, itemPerPage);
                preparedStatement = connection.prepareStatement("SELECT data FROM data WHERE polydata_index LIKE ? ORDER BY RANDOM() LIMIT " + randomCount + " ; ");
            } else {
                preparedStatement = connection.prepareStatement("SELECT data FROM data WHERE polydata_index LIKE ? ORDER BY update_date DESC LIMIT " + (page * itemPerPage) + "," + itemPerPage + " ; ");
            }
            preparedStatement.setString(1, "%|" + index + "|%");
            ResultSet resultSet = preparedStatement.executeQuery();
            while (resultSet.next()) {
                String rawData = resultSet.getString("data");
                BasicPoly basicPoly = objectMapper.readValue(rawData, BasicPoly.class);
                list.add(basicPoly);
            }

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return list;
    }

    @Override
    public Long count(String poly, PolyQuery polyQuery) {
        BasicPolyQuery query = (BasicPolyQuery) polyQuery;
        Optional<BasicPoly> configPoly = config(poly);
        if (configPoly.isEmpty()) {
            throw new RuntimeException("Poly " + poly + " is not configured");
        }
        String index = DATE_INDEX;
        String tag = query.index();
        if (!StringUtils.isBlank(tag)) {
            index = tag;
        }
        try (Connection connection = fetchConnection(poly)) {
            PreparedStatement preparedStatement = null;
            preparedStatement = connection.prepareStatement("SELECT count(data) FROM data WHERE polydata_index LIKE ? ; ");
            preparedStatement.setString(1, "%|" + index + "|%");
            ResultSet resultSet = preparedStatement.executeQuery();
            long count = 0;
            if (resultSet.next()) {
                count = resultSet.getLong(1);
            }
            return count;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public BasicPolyList list() {
        BasicPolyList list = new BasicPolyList();
        File[] files = rootDir.listFiles();
        if (files != null) {
            for (File file : files) {
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

    private void recalculateIndex(String poly) {
        BasicPoly index = BasicPoly.newPoly(INDEX_KEY);
        try (Connection connection = fetchConnection(poly)) {
            PreparedStatement preparedStatement = connection
                    .prepareStatement("SELECT polydata_index FROM data");
            ResultSet resultSet = preparedStatement.executeQuery();
            while (resultSet.next()) {
                String indexString = resultSet.getString("polydata_index");
                String[] split = indexString.split("\\|");
                for (String s : split) {
                    if (s != null && !s.isEmpty()) {
                        BasicPoly data = index.fetch(s, BasicPoly.newPoly(s));
                        int count = data.fetch(COUNT, 0) + 1;
                        data.put(COUNT, count);
                        index.put(s, data);
                    }
                }

            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        persistInternal(poly, index);
    }

    private void persistInternal(String poly, BasicPoly data) {
        try (Connection connection = fetchConnection(poly)) {
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
        try (Connection connection = fetchConnection(poly)) {
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

    private String createQuestionMarks(Set<String> ids) {
        String q = "";
        for (int i = 0; i < ids.size(); i++) {
            q += "?,";
        }
        q = q.substring(0, q.length() - 1);
        return q;
    }

    private String buildTagIndexString(Set<String> indexToPersist) {
        String tagString = "";
        for (String index : indexToPersist) {
            tagString += "|" + index + "|";
        }
        return tagString;
    }
}
