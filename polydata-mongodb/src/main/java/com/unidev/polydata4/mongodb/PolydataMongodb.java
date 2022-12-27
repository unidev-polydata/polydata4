package com.unidev.polydata4.mongodb;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.bulk.BulkWriteResult;
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

/**
 * Storage of polydata records in mongodb.
 */
@Slf4j
public class PolydataMongodb extends AbstractPolydata {
    public static final String CONFIGURATION_COLLECTION = "_config";
    public static final String METADATA_COLLECTION = "_metadata";
    public static final String CREATE_DATE = "_create_date";
    private static final String UPDATE_DATE = "_update_date";
    public static final String INDEX_COLLECTION = "_indexes";
    public static final String COUNT = "count";
    private static final String TAGS = "tags";
    private static final String INDEX = "index";
    private static final String INDEXED_TAGS = "_indexed_tags";

    private static final String DATA = "data";

    private static final String DATE_INDEX = "_date";

    public static final String ITEM_PER_PAGE = "item_per_page";
    public static final int DEFAULT_ITEM_PER_PAGE = 10;

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
        collection(poly).createIndex(Indexes.ascending(INDEXED_TAGS));
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
        if (exists(poly)) {
            return Optional.empty();
        }
        BasicPoly index = new BasicPoly();
        MongoCollection<Document> collection = indexCollection(poly);
        for (Document document : collection.find()) {
            BasicPoly value = toPoly(document);
            index.put(value._id(), value);
        }
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
    public BasicPolyList insert(String poly, Collection<PersistRequest> persistRequests) {
        BasicPolyList basicPolyList = new BasicPolyList();

        Set<String> polyIds = new HashSet<>();
        for (PersistRequest persistRequest : persistRequests) {
            BasicPoly data = persistRequest.getPoly();
            polyIds.add(data._id());

            Set<String> indexToPersist = persistRequest.getIndexToPersist();
            if (CollectionUtils.isEmpty(indexToPersist)) {
                indexToPersist = new HashSet<>();
            } else {
                indexToPersist = new HashSet<>(indexToPersist);
            }
            indexToPersist.add(DATE_INDEX);
            persistRequest.setIndexToPersist(indexToPersist);
        }
        BasicPolyList existingPolys = read(poly, polyIds);

        // bulk insert

        MongoCollection<Document> polydataCollection = collection(poly);

        List<UpdateOneModel<Document>> requests = new ArrayList<>();
        UpdateOptions opt = new UpdateOptions().upsert(true);

        for (PersistRequest persistRequest : persistRequests) {
            BasicPoly data = persistRequest.getPoly();
            String id = data._id();

            Set<String> indexToPersist = persistRequest.getIndexToPersist();
            if (CollectionUtils.isEmpty(indexToPersist)) {
                indexToPersist = new HashSet<>();
            }

            long createDate = System.currentTimeMillis();

            Document polyDocument = toDocument(data);
            polyDocument.put(_ID, id);
            polyDocument.put(CREATE_DATE, createDate);
            polyDocument.put(UPDATE_DATE, createDate);
            polyDocument.put(INDEXED_TAGS, indexToPersist);

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

        // syncronize indexes
        // collect update requests
        Map<String, Integer> tagsToIncrement = new HashMap<>();
        Map<String, BasicPoly> tagsData = new HashMap<>();

        for (PersistRequest persistRequest : persistRequests) {
            BasicPoly data = persistRequest.getPoly();
            Set<String> indexToPersist = persistRequest.getIndexToPersist();
            if (CollectionUtils.isEmpty(indexToPersist)) {
                continue;
            }
            if (existingPolys.hasPoly(data._id())) {
                // skip increment of tags if poly already exists
                continue;
            }
            Map<String, BasicPoly> indexData = persistRequest.getIndexData();
            if (indexData == null) {
                indexData = new HashMap<>();
            }

            for (String index : indexToPersist) {
                int count = tagsToIncrement.getOrDefault(index, 0);
                count++;
                tagsToIncrement.put(index, count);
                tagsData.put(index, indexData.get(index));
            }

        }

        // bulk tag increment increment
        List<UpdateOneModel<Document>> tagsUpdate = new ArrayList<>();
        // bulk update
        for (Map.Entry<String, Integer> tagToIncrement : tagsToIncrement.entrySet()) {
            String index = tagToIncrement.getKey();
            Document indexDocument = new Document();
            indexDocument.put(_ID, index);
            if (tagsData.containsKey(index)) {
                tagsData.put("data", tagsData.get(index));
            }

            // persist index data
            Bson indexUpdate = new Document("$set", indexDocument);
            Bson indexFilter = Filters.eq(_ID, index);
            tagsUpdate.add(new UpdateOneModel<>(indexFilter, indexUpdate, opt));

            // index increment
            Bson inc = new Document("$inc", new Document()
                    .append(COUNT, tagToIncrement.getValue())
            );
            tagsUpdate.add(new UpdateOneModel<>(indexFilter, inc, opt));
        }

        if (!tagsUpdate.isEmpty()) {
            BulkWriteResult bulkWriteResult = indexCollection(poly).bulkWrite(tagsUpdate);
            log.debug(
                    "Poly metadata update result getInsertedCount {} getModifiedCount {} getMatchedCount {}",
                    bulkWriteResult.getInsertedCount(), bulkWriteResult.getModifiedCount(),
                    bulkWriteResult.getMatchedCount());
        }

        return basicPolyList;
    }

    @Override
    public BasicPolyList update(String poly, Collection<PersistRequest> persistRequests) {
        return insert(poly, persistRequests);
    }

    @Override
    public BasicPolyList read(String poly, Set<String> ids) {
        Map<String, BasicPoly> cachedPolys = ifCache(cache -> {
            List<String> cachedIds = new ArrayList<>();
            for (String id : ids) {
                cachedIds.add(poly + "-read-" + id);
            }
            return cache.getAll(ids);
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

        final BasicPolyList dbPolys = new BasicPolyList();
        Bson query = Filters.in(_ID, ids);
        collection(poly).find(query).iterator().forEachRemaining(document -> {
            BasicPoly polyData = toPoly(document);
            dbPolys.add(polyData);
        });
        if (cache.isPresent()) {
            Cache cacheInstance = cache.get();
            for (BasicPoly polyData : dbPolys.list()) {
                cacheInstance.put(poly + "-read-" + polyData._id(), polyData);
            }
        }
        list.list().addAll(dbPolys.list());

        return list;
    }

    @Override
    public BasicPolyList remove(String poly, Set<String> ids) {
        //TODO: update cache
        BasicPolyList basicPolyList = read(poly, ids);

        UpdateOptions opt = new UpdateOptions().upsert(true);
        List<UpdateOneModel<Document>> tagsUpdate = new ArrayList<>();

        for (BasicPoly basicPoly : basicPolyList.list()) {
            Collection<String> tags = basicPoly.fetch(INDEXED_TAGS);

            // decrement indexes...
            Map<String, Integer> tagsToIncrement = new HashMap<>();

            for (String index : tags) {
                int count = tagsToIncrement.getOrDefault(index, 0);
                count++;
                tagsToIncrement.put(index, count);
            }

            for (Map.Entry<String, Integer> tagEntry : tagsToIncrement.entrySet()) {
                String indexId = tagEntry.getKey();

                Bson indexFilter = Filters.eq(_ID, indexId);
                Bson dec = new Document("$inc", new Document().append(COUNT, -1 * tagEntry.getValue()));
                tagsUpdate.add(new UpdateOneModel<>(indexFilter, dec, opt));
            }
        }

        if (!tagsUpdate.isEmpty()) {
            // bulk decrement
            BulkWriteResult bulkWriteResult = indexCollection(poly).bulkWrite(tagsUpdate);
            log.debug("Tags decrement result getInsertedCount {} getModifiedCount {} getMatchedCount {}",
                    bulkWriteResult.getInsertedCount(), bulkWriteResult.getModifiedCount(),
                    bulkWriteResult.getMatchedCount());
        }

        // correct negative numbers
        indexCollection(poly).updateMany(Filters.lt(COUNT, 0),
                new Document("$set", new Document().append(COUNT, 0)));

        // delete by id
        collection(poly).deleteMany(Filters.in(_ID, ids));
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

        Integer defaultItemPerPage = config.fetch(ITEM_PER_PAGE, DEFAULT_ITEM_PER_PAGE);
        Integer itemPerPage = query.getOptions().fetch(ITEM_PER_PAGE, defaultItemPerPage);
        Bson mongoQuery = Filters.in(INDEXED_TAGS, index);
        MongoCollection<Document> collection = collection(poly);
        if (query.queryType() == BasicPolyQuery.QueryFunction.RANDOM) {
            long count = collection(POLY).countDocuments(mongoQuery);
            Random random = new Random();
            for (int i = 0; i < itemPerPage; i++) {
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
            String key = poly + "-query-" + query.page() + "-"+ query.index() + "-" + query.queryType();
            BasicPoly cachedQuery = cache.get(key);
            if (cachedQuery != null) {
                return cachedQuery.fetch("list");
            }
            return null;
        });

        if (cachedResult != null) {
            return cachedResult;
        }

        int page = query.page();
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
            String key = poly + "-query-" + query.page() + "-"+ query.index() + "-" + query.queryType();
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
            String key = poly + "-count-" + query.page() + "-"+ query.index() + "-" + query.queryType();
            BasicPoly cachedQuery = cache.get(key);
            if (cachedQuery != null) {
                return cachedQuery.fetch("count");
            }
            return null;
        });
        if (cachedResult != null) {
            return cachedResult;
        }

        Bson mongoQuery = Filters.in(INDEXED_TAGS, index);
        MongoCollection<Document> collection = collection(poly);
        Long count = collection.countDocuments(mongoQuery);

        if (cache.isPresent()) {
            Cache<String, BasicPoly> cachedInstance = cache.get();
            String key = poly + "-count-" + query.page() + "-"+ query.index() + "-" + query.queryType();
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
        BasicPoly cachedPoly = ifCache(cache -> cache.get(collection + "-poly-from-collection-"+ poly));
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
}
