package com.unidev.polydata4.flatfiles;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.Version;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.module.SimpleModule;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;

/**
 * Flat file deserialization
 */
public class FlatFileDeserializer extends StdDeserializer<FlatFile> {

    public static void install(ObjectMapper objectMapper) {

        SimpleModule flatFile =
                new SimpleModule("FlatFileDeserializer", new Version(1, 0, 0, null, null, null));
        flatFile.addDeserializer(FlatFile.class, new FlatFileDeserializer(FlatFile.class, objectMapper));
        flatFile.addDeserializer(FlatFile.FileMetadata.class, new FlatFileDeserializer.FileMetadataDeserializer(FlatFile.FileMetadata.class, objectMapper));
        objectMapper.registerModule(flatFile);
    }

    private final ObjectMapper objectMapper;

    protected FlatFileDeserializer(Class<?> vc, ObjectMapper objectMapper) {
        super(vc);
        this.objectMapper = objectMapper;
    }

    @Override
    public FlatFile deserialize(JsonParser p, DeserializationContext ctxt) throws IOException {
        FlatFile flatFile = new FlatFile();
        HashMap rawData = p.getCodec().readValue(p, HashMap.class);
        Object rawMetadata = rawData.get("_metadata");
        rawData.remove("_metadata");
        flatFile.putAll(rawData);
        if (rawMetadata != null) {
            FlatFile.FileMetadata metadata = objectMapper.convertValue(rawMetadata, FlatFile.FileMetadata.class);
            flatFile.setMetadata(metadata);
        }
        return flatFile;
    }

    /**
     * Flat files deserializer.
     */
    public static class FileMetadataDeserializer extends StdDeserializer<FlatFile.FileMetadata> {

        private final ObjectMapper objectMapper;

        protected FileMetadataDeserializer(Class<?> vc, ObjectMapper objectMapper) {
            super(vc);
            this.objectMapper = objectMapper;
        }

        @Override
        public FlatFile.FileMetadata deserialize(JsonParser p, DeserializationContext ctxt) throws IOException {
            FlatFile.FileMetadata metadata = new FlatFile.FileMetadata();

            HashMap rawData = p.getCodec().readValue(p, HashMap.class);
            List<String> indexList = (List<String>) rawData.get("_index");
            rawData.remove("_index");
            metadata.putAll(rawData);
            metadata.setIndex(indexList);

            return metadata;
        }
    }
}
