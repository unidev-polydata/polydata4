package com.unidev.polydata4.sqlite;

import com.unidev.polydata4.api.AbstractPolydata;
import com.unidev.polydata4.domain.BasicPoly;
import com.unidev.polydata4.domain.BasicPolyList;
import com.unidev.polydata4.domain.InsertRequest;
import com.unidev.polydata4.domain.PolyQuery;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.flywaydb.core.Flyway;
import org.sqlite.SQLiteDataSource;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Polydata storage backed by SQLite databases.
 */
@RequiredArgsConstructor
@Slf4j
public class PolydataSqlite extends AbstractPolydata {

    private final File rootDir;

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
        config._id(poly + "-" + CONFIG_KEY);
        config.put(ITEM_PER_PAGE, DEFAULT_ITEM_PER_PAGE);
        config(poly, config);
        metadata(poly, BasicPoly.newPoly(poly + "-" + METADATA_KEY));

        return config(poly).get();

    }

    @Override
    public boolean exists(String poly) {
        return false;
    }

    @Override
    public Optional<BasicPoly> config(String poly) {
        return Optional.empty();
    }

    @Override
    public void config(String poly, BasicPoly config) {

    }

    @Override
    public Optional<BasicPoly> metadata(String poly) {
        return Optional.empty();
    }

    @Override
    public void metadata(String poly, BasicPoly metadata) {

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
        return null;
    }

    @Override
    public BasicPolyList update(String poly, Collection<InsertRequest> insertRequests) {
        return null;
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
        return null;
    }

    @Override
    public void open() {

    }

    @Override
    public void close() throws IOException {

    }

    private SQLiteDataSource fetchDataSource(String poly) {
        return connections.computeIfAbsent(poly, k -> {
            File dbFile = new File(rootDir, k + ".db.sqlite");
            SQLiteDataSource sqLiteDataSource = new SQLiteDataSource();
            sqLiteDataSource.setUrl("jdbc:sqlite:" + dbFile.getAbsolutePath());
            return sqLiteDataSource;
        });
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
