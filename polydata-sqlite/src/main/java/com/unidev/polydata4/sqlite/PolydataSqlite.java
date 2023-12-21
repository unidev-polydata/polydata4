package com.unidev.polydata4.sqlite;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.unidev.polydata4.api.AbstractPolydata;
import com.unidev.polydata4.domain.*;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.builder.HashCodeBuilder;
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
        return indexToPersist;
    }

    @Override
    public void prepareStorage() {

    }

    @Override
    public BasicPoly create(String dataset) {
        SQLiteDataSource datasource = fetchDataSource(dataset);
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
        config(dataset, config);
        metadata(dataset, BasicPoly.newPoly(METADATA_KEY));

        return config(dataset).get();

    }

    @Override
    public boolean exists(String dataset) {
        return getDbFile(dataset).exists();
    }

    @Override
    public Optional<BasicPoly> config(String dataset) {
        return readInternal(dataset, CONFIG_KEY);
    }

    @Override
    public void config(String dataset, BasicPoly config) {
        persistInternal(dataset, config);
    }

    @Override
    public Optional<BasicPoly> metadata(String dataset) {
        return readInternal(dataset, METADATA_KEY);
    }

    @Override
    public void metadata(String dataset, BasicPoly metadata) {
        persistInternal(dataset, metadata);
    }

    @Override
    public Optional<BasicPoly> index(String dataset) {
        Optional<BasicPoly> index = readInternal(dataset, INDEX_KEY);
        if (index.isEmpty()) {
            return index;
        }
        BasicPoly internalIndex = index.get();
        BasicPoly result = new BasicPoly();

        Map<String, Object> data = internalIndex.data();
        for (Map.Entry<String, Object> entry : data.entrySet()) {
            if (!(entry.getValue() instanceof Map)) {
                continue;
            }
            Map<String, Object> value = (Map<String, Object>) entry.getValue();
            if (value.containsKey("data")) {
                value = (Map<String, Object>) value.get("data");
            }
            BasicPoly poly = new BasicPoly();
            poly.data().putAll(value);
            result.put(entry.getKey(), poly);
        }
        return Optional.of(result);
    }

    @Override
    public Optional<BasicPoly> indexData(String dataset, String indexId) {
        Optional<BasicPoly> index = index(dataset);
        return index.map(basicPoly -> basicPoly.fetch(indexId));
    }

    @Override
    public BasicPolyList insert(String dataset, InsertOptions insertOptions, Collection<InsertRequest> insertRequests) {
        BasicPolyList result = new BasicPolyList();

        Set<String> ids = new HashSet<>();
        insertRequests.forEach(persistRequest -> ids.add(persistRequest.getData()._id()));
        BasicPolyList existingPolys = read(dataset, ids);

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

        Connection connection = fetchConnection(dataset);
        try {
            connection.setAutoCommit(false);
            PreparedStatement preparedStatement = connection
                    .prepareStatement(
                            "INSERT INTO data(_id_n, _id, data, polydata_index, create_date, update_date) VALUES(?, ?, ?, ?, ?, ?)");

            for (InsertRequest request : toInsert) {
                String id = request.getData()._id();
                long id_n = genHash(id);
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


                    preparedStatement.setLong(1, id_n);
                    preparedStatement.setString(3, jsonData);
                    preparedStatement.setString(4, tagString);
                    preparedStatement.setLong(5, createDate);
                    preparedStatement.setLong(6, updateDate);
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

        if (insertOptions.skipIndex()) {
            return result;
        }

        recalculateIndex(dataset);
        if (!toUpdate.isEmpty()) {
            update(dataset, toUpdate);
        }

        return result;
    }

    @Override
    public BasicPolyList insert(String dataset, Collection<InsertRequest> insertRequests) {
        return insert(dataset, InsertOptions.defaultInsertOptions(), insertRequests);
    }

    @Override
    public BasicPolyList update(String dataset, Collection<InsertRequest> updateRequests) {
        BasicPolyList result = new BasicPolyList();
        Connection connection = fetchConnection(dataset);
        try {
            connection.setAutoCommit(false);
            PreparedStatement preparedStatement = connection
                    .prepareStatement("UPDATE data SET data=?, polydata_index=?, update_date=? WHERE _id_n=?");
            for (InsertRequest request : updateRequests) {
                Set<String> tags = buildTagIndex(request);
                String tagString = buildTagIndexString(tags);
                BasicPoly data = request.getData();
                data.put(INDEXES, tags);
                String id = data._id();
                long id_n = genHash(id);
                String jsonData = objectMapper.writeValueAsString(data);
                preparedStatement.setString(1, jsonData);
                preparedStatement.setString(2, tagString);
                preparedStatement.setLong(3, System.currentTimeMillis());
                preparedStatement.setLong(4, id_n);
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

        recalculateIndex(dataset);

        log.info("Updated polys {} ", updateRequests.size());
        return result;
    }

    @Override
    public BasicPolyList read(String dataset, Set<String> ids) {
        BasicPolyList basicPolyList = new BasicPolyList();
        if (ids.isEmpty()) {
            return basicPolyList;
        }
        Connection connection = fetchConnection(dataset);
        try {
            String q = createQuestionMarks(ids);
            PreparedStatement preparedStatement = connection
                    .prepareStatement("SELECT data FROM data WHERE _id_n IN ( " + q + ") ; ");
            int i = 1;
            for (String id : ids) {
                preparedStatement.setLong(i++, genHash(id));
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
    public BasicPolyList remove(String dataset, Set<String> ids) {
        BasicPolyList basicPolyList = read(dataset, ids);
        Connection connection = fetchConnection(dataset);
        try {
            String q = createQuestionMarks(ids);
            PreparedStatement preparedStatement = connection
                    .prepareStatement("DELETE FROM data WHERE _id_n IN (" + q + ") ; ");
            int i = 1;
            for (String id : ids) {
                preparedStatement.setLong(i++, genHash(id));
            }
            long removedRows = preparedStatement.executeUpdate();
            log.info("Removed {} rows", removedRows);
            recalculateIndex(dataset);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return basicPolyList;
    }

    @Override
    public BasicPolyList query(String dataset, PolyQuery polyQuery) {
        BasicPolyQuery query = (BasicPolyQuery) polyQuery;
        Optional<BasicPoly> configPoly = config(dataset);

        if (configPoly.isEmpty()) {
            throw new RuntimeException("Poly " + dataset + " is not configured");
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
        Connection connection = fetchConnection(dataset);
        try {
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
    public Long count(String dataset, PolyQuery polyQuery) {
        BasicPolyQuery query = (BasicPolyQuery) polyQuery;
        Optional<BasicPoly> configPoly = config(dataset);
        if (configPoly.isEmpty()) {
            throw new RuntimeException("Poly " + dataset + " is not configured");
        }
        String index = DATE_INDEX;
        String tag = query.index();
        if (!StringUtils.isBlank(tag)) {
            index = tag;
        }
        Connection connection = fetchConnection(dataset);
        try {
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
                    String dataset = file.getName().replace(DB_FILE_EXTENSION, "");
                    list.add(BasicPoly.newPoly(dataset));
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
        connectionMap.values().forEach(connection -> {
            try {
                connection.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        });
    }

    private void recalculateIndex(String dataset) {
        BasicPoly index = BasicPoly.newPoly(INDEX_KEY);
        Connection connection = fetchConnection(dataset);
        try {
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
        persistInternal(dataset, index);
    }

    private void persistInternal(String dataset, BasicPoly data) {
        Connection connection = fetchConnection(dataset);
        try {
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

    private Optional<BasicPoly> readInternal(String dataset, String id) {
        Connection connection = fetchConnection(dataset);
        try {
            PreparedStatement preparedStatement = connection
                    .prepareStatement("SELECT data FROM internal WHERE _id=?");
            preparedStatement.setString(1, id);

            ResultSet resultSet = preparedStatement.executeQuery();
            if (resultSet.next()) {
                String rawData = resultSet.getString("data");
                return Optional.ofNullable(objectMapper.readValue(rawData, BasicPoly.class));
            }
        }catch (Exception e) {
            e.printStackTrace();
        }
        return Optional.empty();
    }

    private SQLiteDataSource fetchDataSource(String dataset) {
        return connections.computeIfAbsent(dataset, k -> {
            File dbFile = getDbFile(k);
            SQLiteDataSource sqLiteDataSource = new SQLiteDataSource();
            sqLiteDataSource.setUrl("jdbc:sqlite:" + dbFile.getAbsolutePath());
            return sqLiteDataSource;
        });
    }

    private File getDbFile(String dataset) {
        File dbFile = new File(rootDir, dataset + DB_FILE_EXTENSION);
        return dbFile;
    }

    private final Map<String, Connection> connectionMap = new ConcurrentHashMap<>();

    private Connection fetchConnection(String dataset) {
        return connectionMap.computeIfAbsent(Thread.currentThread().getName() + "-" + dataset, k -> {
            try {
                return fetchDataSource(dataset).getConnection();
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        });
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

    long genHash(String value) {
        return new HashCodeBuilder(17, 37)
                .append(value)
                .toHashCode() & 0xffffffffL;
    }

}
