package com.unidev.polydata4.flatfiles;

import com.fasterxml.jackson.core.JacksonException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;

import java.io.IOException;
import java.util.HashMap;

/**
 * Flat file deserialization
 */
public class FlatFileDeserializer extends StdDeserializer<FlatFile> {

    private final ObjectMapper objectMapper;

    protected FlatFileDeserializer(Class<?> vc, ObjectMapper objectMapper) {
        super(vc);
        this.objectMapper = objectMapper;
    }

    @Override
    public FlatFile deserialize(JsonParser p, DeserializationContext ctxt) throws IOException, JacksonException {
        FlatFile flatFile = new FlatFile();
        HashMap rawData = p.getCodec().readValue(p, HashMap.class);
        Object rawMetadata = rawData.get("_metadata");
        rawData.remove("_metadata");
        flatFile.putAll(rawData);
        if (rawMetadata != null) {
            FlatFile.YamlFileMetadata metadata = objectMapper.convertValue(rawMetadata, FlatFile.YamlFileMetadata.class);
            flatFile.setMetadata(metadata);
        }
        return flatFile;
    }
}
