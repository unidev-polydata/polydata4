package com.unidev.polydata4.mongodb;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Indexes;
import com.mongodb.client.model.UpdateOneModel;
import com.mongodb.client.model.UpdateOptions;
import com.unidev.platform.common.exception.UnidevRuntimeException;
import com.unidev.polydata4.api.Polydata;
import com.unidev.polydata4.domain.*;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.bson.Document;
import org.bson.conversions.Bson;

import java.io.IOException;
import java.util.*;

/**
 * Storage of polydata records in mongodb.
 */
@Slf4j
public class PolydataMongodb implements Polydata {
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

    String ITEM_PER_PAGE = "item_per_page";
    int DEFAULT_ITEM_PER_PAGE = 10;

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
        collection(INDEX_COLLECTION).createIndex(Indexes.ascending(POLY, INDEX));
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
    public BasicPoly index(String poly) {
        BasicPoly index = new BasicPoly();
        MongoCollection<Document> collection = collection(INDEX_COLLECTION);
        for (Document document : collection.find(Filters.eq(POLY, poly))) {
            BasicPoly value = toPoly(document);
            index.put(value.fetch(INDEX), value);
        }
        return index;
    }

    @Override
    public BasicPoly indexData(String poly, String indexId) {
       return index(poly).fetch(indexId);
    }

    @Override
    public BasicPolyList insert(String poly, Collection<PersistRequest> persistRequests) {
        BasicPolyList basicPolyList = new BasicPolyList();


        Set<String> polyIds = new HashSet<>();
        for (PersistRequest persistRequest : persistRequests) {
            BasicPoly data = persistRequest.getPoly();
            polyIds.add(data._id());
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
            if (indexToPersist == null) {
                indexToPersist = new HashSet<>();
            }

            Document polyDocument = new Document();
            polyDocument.put(_ID, id);
            polyDocument.put(POLY_ID, data._id());
            polyDocument.put(CREATE_DATE, new Date());
            polyDocument.put(UPDATE_DATE, new Date());
            polyDocument.put(INDEXED_TAGS, indexToPersist);

            Document document = toDocument(data);

            polyDocument.append(DATA, document);
            polyDocument.append(TAGS, indexToPersist);

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
//
//        // bulk tag increment increment
//        List<UpdateOneModel<Document>> tagsUpdate = new ArrayList<>();
//
//        // collect update requests
//        Map<String, Integer> tagsToIncrement = new HashMap<>();
//        Map<String, BasicPoly> tagsData = new HashMap<>();
//
//        for (PersistRequest persistRequest : persistRequests) {
//            BasicPoly poly = persistRequest.getPoly();
//            Set<String> indexToPersist = persistRequest.getIndexToPersist();
//            if (CollectionUtils.isEmpty(indexToPersist)) {
//                continue;
//            }
//            if (existingPolys.hasPoly(poly._id())) {
//                // skip increment of tags if poly already exists
//                continue;
//            }
//            Map<String, BasicPoly> indexData = persistRequest.getIndexData();
//            if (indexData == null) {
//                indexData = new HashMap<>();
//            }
//
//            for (String index : indexToPersist) {
//                int count = tagsToIncrement.getOrDefault(index, 0);
//                count++;
//                tagsToIncrement.put(index, count);
//                tagsData.put(index, indexData.get(index));
//            }
//
//        }
//
//        // bulk update
//
//        for (Map.Entry<String, Integer> tagToIncrement : tagsToIncrement.entrySet()) {
//            String index = tagToIncrement.getKey();
//            String indexId = name + "-" + index;
//            Document indexDocument = new Document();
//            indexDocument.put(_ID, indexId);
//            indexDocument.put(POLY, name);
//            indexDocument.put("index", index);
//            if (tagsData.containsKey(index)) {
//                tagsData.put("data", tagsData.get(index));
//            }
//
//            // persist index data
//            Bson indexUpdate = new Document("$set", indexDocument);
//            Bson indexFilter = Filters.eq(_ID, indexId);
//            tagsUpdate.add(new UpdateOneModel<>(indexFilter, indexUpdate, opt));
//
//            // index increment
//            Bson inc = new Document("$inc", new Document().append(COUNT, tagToIncrement.getValue()));
//            tagsUpdate.add(new UpdateOneModel<>(indexFilter, inc, opt));
//        }
//
//        if (!tagsUpdate.isEmpty()) {
//            BulkWriteResult bulkWriteResult = getCollection(TAGS_INDEX_COLLECTION).bulkWrite(tagsUpdate);
//            log.debug(
//                    "Poly metadata update result getInsertedCount {} getModifiedCount {} getMatchedCount {}",
//                    bulkWriteResult.getInsertedCount(), bulkWriteResult.getModifiedCount(),
//                    bulkWriteResult.getMatchedCount());
//        }
//

        return basicPolyList;
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
      /*  BasicPolyQuery q = (BasicPolyQuery) polyQuery;
        Optional<BasicPoly> configPoly = config(poly);
        BasicPolyList list = new BasicPolyList();
        if (configPoly.isEmpty()) {
            return list;
        }

        String index = "_date";
        if (StringUtils.isNotBlank(q.index())) {
            index = q.index();
        }

        BasicPoly config = configPoly.get();

        Integer defaultItemPerPage = config.fetch(ITEM_PER_PAGE, DEFAULT_ITEM_PER_PAGE);
        Integer itemPerPage = polyQuery.getOptions().fetch(ITEM_PER_PAGE, defaultItemPerPage);
        Bson query = Filters.and(Filters.eq(POLY, name), Filters.in(TAGS, index)); */

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
