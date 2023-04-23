package com.unidev.polydata4.mongodb;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.*;
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
import java.util.stream.Collectors;

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

    public void prepareStorage(String poly) {
        collection(poly).createIndex(Indexes.ascending(INDEXES));
        indexCollection(poly).createIndex(Indexes.descending(COUNT));
    }

    @Override
    public void prepareStorage() {

    }

    @Override
    public BasicPoly create(String poly) {
        if (exists(poly)) {
            return config(poly).get();
        }
        config(poly, BasicPoly.newPoly(poly));
        metadata(poly, BasicPoly.newPoly(poly).with(CREATE_DATE, new Date()));

        prepareStorage(poly);

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
        if (!exists(poly)) {
            return Optional.empty();
        }
        BasicPoly cachedResult = ifCache(cache -> {
            String key = poly + "-index";
            return cache.get(key);
        });
        if (cachedResult != null) {
            return Optional.of(cachedResult);
        }
        BasicPoly index = new BasicPoly();
        MongoCollection<Document> collection = indexCollection(poly);
        for (Document document : collection.find()) {
            BasicPoly value = toPoly(document);
            index.put(value._id(), value);
        }
        putIfCache(poly + "-index", index);
        return Optional.of(index);
    }

    @Override
    public Optional<BasicPoly> indexData(String poly, String indexId) {
        Optional<BasicPoly> index = index(poly);
        if (index.isEmpty()) {
            return Optional.empty();
        }
        return index.get().fetch(indexId);
    }

    @Override
    public BasicPolyList insert(String poly, Collection<InsertRequest> insertRequests) {
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
            indexToPersist.add(DATE_INDEX);
            insertRequest.setIndexToPersist(indexToPersist);
        }
        // bulk insert
        MongoCollection<Document> polydataCollection = collection(poly);

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
        recalculateIndex(poly);
        return basicPolyList;
    }

    @Override
    public BasicPolyList update(String poly, Collection<InsertRequest> insertRequests) {
        return insert(poly, insertRequests);
    }

    @Override
    public BasicPolyList read(String poly, Set<String> ids) {
        Map<String, BasicPoly> cachedPolys = ifCache(cache -> {
            Set<String> cachedIds = new HashSet<>();
            for (String id : ids) {
                cachedIds.add(poly + "-read-" + id);
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
        try (MongoCursor<Document> cursor = collection(poly).find(query).iterator()) {
            cursor.forEachRemaining(document -> {
                BasicPoly polyData = toPoly(document);
                dbPolys.add(polyData);
            });
        }
        if (cache.isPresent()) {
            Cache cacheInstance = cache.get();
            Map<String, BasicPoly> cacheMap = new HashMap<>();
            for (BasicPoly data : dbPolys.list()) {
                cacheMap.put(poly + "-read-" + data._id(), data);
            }
            cacheInstance.putAll(cacheMap);
        }
        list.list().addAll(dbPolys.list());

        return list;
    }

    @Override
    public BasicPolyList remove(String poly, Set<String> ids) {
        BasicPolyList list = read(poly, ids);
        // delete by id
        collection(poly).deleteMany(Filters.in(_ID, ids));
        recalculateIndex(poly);
        if (cache.isPresent()) {
            Cache cacheInstance = cache.get();
            Set<String> keysToRemove = new HashSet<>();
            for (BasicPoly data : list.list()) {
                keysToRemove.add(poly + "-read-" + data._id());
            }
            cacheInstance.removeAll(keysToRemove);
        }
        return list;
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
        Bson mongoQuery = Filters.in(INDEXES, index);
        MongoCollection<Document> collection = collection(poly);
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
            String key = poly + "-query-" + page + "-" + query.index() + "-" + query.queryType();
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
            String key = poly + "-query-" + query.page() + "-" + query.index() + "-" + query.queryType();
            BasicPoly cachedQuery = new BasicPoly();
            cachedQuery.put("list", list);
            cachedInstance.put(key, cachedQuery);
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

        Long cachedResult = ifCache(cache -> {
            String key = poly + "-count-" + query.index() + "-" + query.queryType();
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
        MongoCollection<Document> collection = collection(poly);
        Long count = collection.countDocuments(mongoQuery);

        if (cache.isPresent()) {
            Cache<String, BasicPoly> cachedInstance = cache.get();
            String key = poly + "-count-" + query.index() + "-" + query.queryType();
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
    private Optional<BasicPoly> fetchPolyFromCollection(String poly, String collection) {
        BasicPoly cachedPoly = ifCache(cache -> cache.get(collection + "-poly-from-collection-" + poly));
        if (cachedPoly != null) {
            return Optional.of(cachedPoly);
        }
        FindIterable<Document> configById = collection(collection)
                .find(Filters.eq(poly));
        try (MongoCursor<Document> iterator = configById.iterator()) {
            if (!iterator.hasNext()) {
                return Optional.empty();
            }
            Document document = iterator.next();
            BasicPoly extractedPoly = toPoly(document);
            putIfCache(collection + "-poly-from-collection-" + poly, extractedPoly);
            return Optional.of(extractedPoly);
        }
    }

    private void persistPolyToCollection(String poly, String collection, BasicPoly data) {
        Document document = toDocument(data);
        document.put(_ID, poly);
        Bson update = new Document("$set", document);
        Bson filter = Filters.eq(_ID, poly);
        UpdateOptions options = new UpdateOptions().upsert(true);
        collection(collection).updateOne(filter, update, options);
        putIfCache(collection + "-poly-from-collection-" + poly, data);
    }

    private MongoCollection<Document> indexCollection(String poly) {
        return collection(INDEX_COLLECTION + "_" + poly);
    }

    private void recalculateIndex(String poly) {
        MongoCollection<Document> collection = collection(poly);
        AggregateIterable<Document> documents = collection.aggregate(
                Arrays.asList(
                        Aggregates.project(Projections.fields(Projections.include(INDEXES))),
                        Aggregates.unwind("$" + INDEXES),
                        Aggregates.group("$" + INDEXES, Accumulators.sum("count", 1))
                )
        );


        try (MongoCursor<Document> iterator = documents.iterator()) {
            List<BasicPoly> indexes = new ArrayList<>();
            while (iterator.hasNext()) {
                Document next = iterator.next();
                String index = next.getString(_ID);
                Long count = Long.parseLong(next.get("count") + "");
                BasicPoly indexData = BasicPoly.newPoly(index);
                indexData.put("count", count);
                indexData.put("index", index);
                indexData.put("poly", poly);
                indexes.add(indexData);
            }
            if (!indexes.isEmpty()) {
                indexCollection(poly).drop();
                indexCollection(poly).insertMany(indexes.stream().map(this::toDocument).collect(Collectors.toList()));
            }
        }
    }

}
