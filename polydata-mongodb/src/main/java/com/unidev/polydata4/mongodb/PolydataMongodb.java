package com.unidev.polydata4.mongodb;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Indexes;
import com.mongodb.client.model.UpdateOptions;
import com.unidev.polydata4.api.Polydata;
import com.unidev.polydata4.domain.BasicPoly;
import com.unidev.polydata4.domain.BasicPolyList;
import com.unidev.polydata4.domain.PersistRequest;
import com.unidev.polydata4.domain.PolyQuery;
import lombok.Getter;
import lombok.Setter;
import org.bson.Document;
import org.bson.conversions.Bson;

import java.io.IOException;
import java.util.*;

/**
 * Storage of polydata records in mongodb.
 */
public class PolydataMongodb implements Polydata {

    public static final String CONFIGURATION_COLLECTION = "_config";
    public static final String METADATA_COLLECTION = "_metadata";
    public static final String CREATE_DATE = "_create_date";

    private static final String UPDATE_DATE = "_update_date";

    public static final String INDEX_COLLECTION = "_indexes";
    public static final String COUNT = "count";

    private static final String TAGS = "tags";
    private static final String INDEX = "index";


    @Getter
    @Setter
    private String mongoUri;

    @Getter
    @Setter
    private MongoClientURI mongoClientURI;

    @Getter
    @Setter
    private MongoClient mongoClient;

    public PolydataMongodb(String mongoUri) {
        this.mongoUri = mongoUri;
        this.mongoClientURI = new MongoClientURI(this.mongoUri);
        this.mongoClient = new MongoClient(mongoClientURI);
    }

    public void prepareStorage() {
        collection(INDEX_COLLECTION).createIndex(Indexes.ascending(POLY));
        collection(INDEX_COLLECTION).createIndex(Indexes.ascending(POLY, TAGS));

        collection(INDEX_COLLECTION).createIndex(Indexes.ascending(POLY));
    }

    @Override
    public BasicPoly create(String poly) {
        if (exists(poly)) {
            return config(poly).get();
        }
        config(poly, BasicPoly.newPoly(poly));
        metadata(poly, BasicPoly.newPoly(poly).with(CREATE_DATE, new Date()));

        return config(poly).get();
    }

    @Override
    public boolean exists(String poly) {
        return config(poly).isPresent();
    }

    @Override
    public Optional<BasicPoly> config(String poly) {
        return fetchPolyFromCollection(poly, CONFIGURATION_COLLECTION);
    }

    @Override
    public void config(String poly, BasicPoly config) {
        persistPolyToCollection(poly, CONFIGURATION_COLLECTION, config);
    }

    @Override
    public Optional<BasicPoly> metadata(String poly) {
        return fetchPolyFromCollection(poly, METADATA_COLLECTION);
    }

    @Override
    public void metadata(String poly, BasicPoly metadata) {
        persistPolyToCollection(poly, METADATA_COLLECTION, metadata);
    }

    @Override
    public Optional<BasicPoly> index(String poly) {
        BasicPoly index = new BasicPoly();
        MongoCollection<Document> collection = collection(INDEX_COLLECTION);
        for (Document document : collection.find(Filters.eq(POLY, poly))) {
            BasicPoly value = toPoly(document);
            index.put(value.fetch(INDEX), value.fetch(COUNT));
        }
        return Optional.of(index);
    }

    @Override
    public BasicPoly indexData(String poly, String indexId) {

    }

    @Override
    public BasicPolyList insert(String poly, Collection<PersistRequest> persistRequests) {
        return null;
    }

    @Override
    public BasicPolyList update(String poly, Collection<PersistRequest> persistRequests) {
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
        BasicPolyList list = new BasicPolyList();
        for (Document document : collection(CONFIGURATION_COLLECTION).find()) {
            list.add(BasicPoly.newPoly(document.getString(_ID)));
        }
        return list;
    }

    @Override
    public void open() {

    }

    @Override
    public void close() throws IOException {

    }

    private MongoCollection<Document> collection(String collection) {
        return mongoClient.getDatabase(Objects.requireNonNull(mongoClientURI.getDatabase())).getCollection(collection);
    }

    private BasicPoly toPoly(Document document) {
        BasicPoly poly = new BasicPoly();
        for (String key : document.keySet()) {
            poly.put(key, document.get(key));
        }
        if (poly.containsKey(POLY_ID)) {
            poly._id(poly.fetch(POLY_ID) + "");
        }
        return poly;
    }

    private Document toDocument(BasicPoly poly) {
        Document document = new Document();
        document.putAll(poly.data());
        return document;
    }


    private Optional<BasicPoly> fetchPolyFromCollection(String poly, String collection) {
        FindIterable<Document> configById = collection(collection)
                .find(Filters.eq(poly));
        try (MongoCursor<Document> iterator = configById.iterator()) {
            if (!iterator.hasNext()) {
                return Optional.empty();
            }
            Document document = iterator.next();
            BasicPoly config = toPoly(document);
            return Optional.of(config);
        }
    }

    private void persistPolyToCollection(String poly, String collection, BasicPoly data) {
        Document document = toDocument(data);
        document.put(_ID, poly);
        Bson update = new Document("$set", document);
        Bson filter = Filters.eq(_ID, poly);
        UpdateOptions options = new UpdateOptions().upsert(true);
        collection(collection).updateOne(filter, update, options);
    }
}
