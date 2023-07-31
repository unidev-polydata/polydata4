package com.unidev.polydata4.mongodb;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.*;
import com.mongodb.client.result.UpdateResult;
import com.unidev.polydata4.api.AbstractPolydata;
import com.unidev.polydata4.domain.*;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.bson.Document;
import org.bson.conversions.Bson;

import javax.cache.Cache;
import java.io.IOException;
import java.util.*;

/**
 * Storage of polydata records in mongodb.
 */
@Slf4j
public class PolydataMongodb extends AbstractPolydata {
    public static final String CONFIGURATION_COLLECTION = "_config";
    public static final String METADATA_COLLECTION = "_metadata";
    public static final String CREATE_DATE = "_create_date";
    public static final String INDEX_COLLECTION = "_indexes";
    private static final String UPDATE_DATE = "_update_date";


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

    public void prepareStorage(String dataset) {
        collection(dataset).createIndex(Indexes.ascending(INDEXES));
        collection(dataset).createIndex(Indexes.ascending(CREATE_DATE));
        collection(dataset).createIndex(Indexes.descending(UPDATE_DATE));
        collection(dataset).createIndex(Indexes.compoundIndex(Indexes.ascending(INDEXES), Indexes.descending(UPDATE_DATE)));
    }

    @Override
    public void prepareStorage() {

    }

    @Override
    public BasicPoly create(String dataset) {
        if (exists(dataset)) {
            return config(dataset).get();
        }
        config(dataset, BasicPoly.newPoly(dataset));
        metadata(dataset, BasicPoly.newPoly(dataset).with(CREATE_DATE, new Date()));

        prepareStorage(dataset);

        return config(dataset).get();
    }

    @Override
    public boolean exists(String dataset) {
        return config(dataset).isPresent();
    }

    @Override
    public Optional<BasicPoly> config(String dataset) {
        return fetchPolyFromCollection(dataset, CONFIGURATION_COLLECTION);
    }

    @Override
    public void config(String dataset, BasicPoly config) {
        persistPolyToCollection(dataset, CONFIGURATION_COLLECTION, config);
    }

    @Override
    public Optional<BasicPoly> metadata(String dataset) {
        return fetchPolyFromCollection(dataset, METADATA_COLLECTION);
    }

    @Override
    public void metadata(String dataset, BasicPoly metadata) {
        persistPolyToCollection(dataset, METADATA_COLLECTION, metadata);
    }

    @Override
    public Optional<BasicPoly> index(String dataset) {
        if (!exists(dataset)) {
            return Optional.empty();
        }
        BasicPoly cachedResult = ifCache(cache -> cache.get(dataset + "-index"));
        if (cachedResult != null) {
            return Optional.of(cachedResult);
        }
        BasicPoly index = null;
        BasicPoly rawIndex = null;
        Bson query = Filters.eq(_ID, dataset);
        try (MongoCursor<Document> cursor = indexCollection(dataset).find(query).iterator()) {
            if (cursor.hasNext()) {
                Document document = cursor.next();
                rawIndex = toPoly(document);
            }
        }
        if (rawIndex != null) {
            // transform index to poly
            index = BasicPoly.newPoly(dataset);
            for (String key : rawIndex.data().keySet()) {
                if (StringUtils.equals(key, _ID)) {
                    continue;
                }
                index.put(key, BasicPoly.newPoly(key).with("count", Long.parseLong(rawIndex.data().get(key) + "")));
            }
            putIfCache(dataset + "-index", index);
        }

        return Optional.ofNullable(index);
    }

    @Override
    public Optional<BasicPoly> indexData(String dataset, String indexId) {
        Optional<BasicPoly> index = index(dataset);
        if (index.isEmpty()) {
            return Optional.empty();
        }
        return index.get().fetch(indexId);
    }

    @Override
    public BasicPolyList insert(String dataset, InsertOptions insertOptions, Collection<InsertRequest> insertRequests) {
        BasicPolyList basicPolyList = new BasicPolyList();

        Set<String> polyIds = new HashSet<>();
        for (InsertRequest insertRequest : insertRequests) {
            BasicPoly data = insertRequest.getData();
            polyIds.add(data._id());

            Set<String> indexToPersist = insertRequest.getIndexToPersist();
            if (CollectionUtils.isEmpty(indexToPersist)) {
                indexToPersist = new HashSet<>();
            } else {
                indexToPersist = new HashSet<>(indexToPersist);
            }
            insertRequest.setIndexToPersist(indexToPersist);
        }
        // bulk insert
        MongoCollection<Document> polydataCollection = collection(dataset);

        List<UpdateOneModel<Document>> requests = new ArrayList<>();
        UpdateOptions opt = new UpdateOptions().upsert(true);

        for (InsertRequest insertRequest : insertRequests) {
            BasicPoly data = insertRequest.getData();
            String id = data._id();

            Set<String> indexToPersist = insertRequest.getIndexToPersist();
            if (CollectionUtils.isEmpty(indexToPersist)) {
                indexToPersist = new HashSet<>();
            }

            long createDate = System.currentTimeMillis();

            Document polyDocument = toDocument(data);
            polyDocument.put(_ID, id);
            polyDocument.put(CREATE_DATE, createDate);
            polyDocument.put(UPDATE_DATE, createDate);
            polyDocument.put(INDEXES, indexToPersist);

            Bson update = new Document("$set", polyDocument);
            Bson filter = Filters.eq(_ID, id);
            requests.add(new UpdateOneModel<>(filter, update, opt));

            basicPolyList.add(data);
        }

        if (!requests.isEmpty()) {
            BulkWriteResult bulkWriteResult = polydataCollection.bulkWrite(requests);
            log.debug("Poly write result getInsertedCount {} getModifiedCount {} getMatchedCount {}",
                    bulkWriteResult.getInsertedCount(), bulkWriteResult.getModifiedCount(),
                    bulkWriteResult.getMatchedCount());
        }

        if (insertOptions.skipIndex()) {
            recalculateIndex(dataset);
        }
        return basicPolyList;
    }

    @Override
    public BasicPolyList insert(String dataset, Collection<InsertRequest> insertRequests) {
        return insert(dataset, InsertOptions.defaultInsertOptions(), insertRequests);
    }

    @Override
    public BasicPolyList update(String dataset, Collection<InsertRequest> insertRequests) {
        return insert(dataset, insertRequests);
    }

    @Override
    public BasicPolyList read(String dataset, Set<String> ids) {
        Map<String, BasicPoly> cachedPolys = ifCache(cache -> {
            Set<String> cachedIds = new HashSet<>();
            for (String id : ids) {
                cachedIds.add(dataset + "-read-" + id);
            }
            return cache.getAll(cachedIds);
        });

        List<String> idsToQuery = new ArrayList<>(ids);
        BasicPolyList list = new BasicPolyList();

        // query from cache only missing ids

        if (cachedPolys != null) {
            for (BasicPoly item : cachedPolys.values()) {
                list.add(item);
                idsToQuery.remove(item._id());
            }
        }

        if (idsToQuery.isEmpty()) {
            return list;
        }

        final BasicPolyList dbPolys = new BasicPolyList();
        Bson query = Filters.in(_ID, idsToQuery);
        try (MongoCursor<Document> cursor = collection(dataset).find(query).iterator()) {
            cursor.forEachRemaining(document -> {
                BasicPoly polyData = toPoly(document);
                dbPolys.add(polyData);
            });
        }
        if (cache.isPresent()) {
            Cache cacheInstance = cache.get();
            Map<String, BasicPoly> cacheMap = new HashMap<>();
            for (BasicPoly data : dbPolys.list()) {
                cacheMap.put(dataset + "-read-" + data._id(), data);
            }
            cacheInstance.putAll(cacheMap);
        }
        list.list().addAll(dbPolys.list());

        return list;
    }

    @Override
    public BasicPolyList remove(String dataset, Set<String> ids) {
        BasicPolyList list = read(dataset, ids);
        // delete by id
        collection(dataset).deleteMany(Filters.in(_ID, ids));
        recalculateIndex(dataset);
        if (cache.isPresent()) {
            Cache cacheInstance = cache.get();
            Set<String> keysToRemove = new HashSet<>();
            for (BasicPoly data : list.list()) {
                keysToRemove.add(dataset + "-read-" + data._id());
            }
            cacheInstance.removeAll(keysToRemove);
        }
        return list;
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
        Bson mongoQuery = Filters.in(INDEXES, index);
        MongoCollection<Document> collection = collection(dataset);
        if (query.queryType() == BasicPolyQuery.QueryFunction.RANDOM) {
            int randomCount = query.option(RANDOM_COUNT, itemPerPage);
            long count = collection.countDocuments(mongoQuery);
            Random random = new Random();
            for (int i = 0; i < randomCount; i++) {
                int skip = random.nextInt((int) count);
                try (MongoCursor<Document> iterator = collection.find(mongoQuery).skip(skip)
                        .iterator()) {
                    if (iterator.hasNext()) {
                        Document next = iterator.next();
                        list.add(toPoly(next));
                    }
                }
            }
            return list;
        }
        BasicPolyList cachedResult = ifCache(cache -> {
            String key = dataset + "-query-" + page + "-" + query.index() + "-" + query.queryType();
            BasicPoly cachedQuery = cache.get(key);
            if (cachedQuery != null) {
                return cachedQuery.fetch("list");
            }
            return null;
        });

        if (cachedResult != null) {
            return cachedResult;
        }

        try (MongoCursor<Document> iterator = collection.find(mongoQuery).sort(
                        Sorts.descending(UPDATE_DATE))
                .skip(page * itemPerPage).limit(itemPerPage).cursor()) {
            while (iterator.hasNext()) {
                Document next = iterator.next();
                list.add(toPoly(next));
            }
        }

        if (cache.isPresent()) {
            Cache<String, BasicPoly> cachedInstance = cache.get();
            String key = dataset + "-query-" + query.page() + "-" + query.index() + "-" + query.queryType();
            BasicPoly cachedQuery = new BasicPoly();
            cachedQuery.put("list", list);
            cachedInstance.put(key, cachedQuery);
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

        Long cachedResult = ifCache(cache -> {
            String key = dataset + "-count-" + query.index() + "-" + query.queryType();
            BasicPoly cachedQuery = cache.get(key);
            if (cachedQuery != null) {
                return cachedQuery.fetch("count");
            }
            return null;
        });
        if (cachedResult != null) {
            return cachedResult;
        }

        Bson mongoQuery = Filters.in(INDEXES, index);
        MongoCollection<Document> collection = collection(dataset);
        Long count = collection.countDocuments(mongoQuery);

        if (cache.isPresent()) {
            Cache<String, BasicPoly> cachedInstance = cache.get();
            String key = dataset + "-count-" + query.index() + "-" + query.queryType();
            BasicPoly cachedQuery = new BasicPoly();
            cachedQuery.put("count", count);
            cachedInstance.put(key, cachedQuery);
        }

        return count;
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
        mongoClient.close();
    }

    private MongoCollection<Document> collection(String collection) {
        return mongoClient.getDatabase(Objects.requireNonNull(mongoClientURI.getDatabase())).getCollection(collection);
    }

    private BasicPoly toPoly(Document document) {
        BasicPoly poly = new BasicPoly();
        for (String key : document.keySet()) {
            poly.put(key, document.get(key));
        }
        return poly;
    }

    private Document toDocument(BasicPoly poly) {
        Document document = new Document();
        document.putAll(poly.data());
        return document;
    }

    /**
     * Fetch Poly document from mongo collection
     */
    private Optional<BasicPoly> fetchPolyFromCollection(String dataset, String collection) {
        BasicPoly cachedPoly = ifCache(cache -> cache.get(collection + "-poly-from-collection-" + dataset));
        if (cachedPoly != null) {
            return Optional.of(cachedPoly);
        }
        FindIterable<Document> configById = collection(collection)
                .find(Filters.eq(dataset));
        try (MongoCursor<Document> iterator = configById.iterator()) {
            if (!iterator.hasNext()) {
                return Optional.empty();
            }
            Document document = iterator.next();
            BasicPoly extractedPoly = toPoly(document);
            putIfCache(collection + "-poly-from-collection-" + dataset, extractedPoly);
            return Optional.of(extractedPoly);
        }
    }

    private void persistPolyToCollection(String dataset, String collection, BasicPoly data) {
        Document document = toDocument(data);
        document.put(_ID, dataset);
        Bson update = new Document("$set", document);
        Bson filter = Filters.eq(_ID, dataset);
        UpdateOptions options = new UpdateOptions().upsert(true);
        collection(collection).updateOne(filter, update, options);
        putIfCache(collection + "-poly-from-collection-" + dataset, data);
    }

    private MongoCollection<Document> indexCollection(String dataset) {
        return collection(INDEX_COLLECTION);
    }

    private void recalculateIndex(String dataset) {
        MongoCollection<Document> collection = collection(dataset);
        AggregateIterable<Document> documents = collection.aggregate(
                Arrays.asList(
                        Aggregates.project(Projections.fields(Projections.include(INDEXES))),
                        Aggregates.unwind("$" + INDEXES),
                        Aggregates.group("$" + INDEXES, Accumulators.sum("count", 1))
                )
        ).allowDiskUse(true);

        try (MongoCursor<Document> iterator = documents.iterator()) {
            BasicPoly indexes = BasicPoly.newPoly(dataset);
            while (iterator.hasNext()) {
                Document next = iterator.next();
                String index = next.getString(_ID);
                Long count = Long.parseLong(String.valueOf(next.get("count")));
                indexes.put(index, count);
            }
            Document indexDocument = toDocument(indexes);
            Bson filter = Filters.eq(_ID, dataset);
            indexCollection(dataset).replaceOne(filter, indexDocument, new ReplaceOptions().upsert(true));
        }
    }

}
