package com.unidev.polydata4.flatfiles;

import com.fasterxml.jackson.core.JacksonException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;

/**
 * Flat files deserializer.
 */
public class FileMetadataDeserializer extends StdDeserializer<FlatFile.FileMetadata> {

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
