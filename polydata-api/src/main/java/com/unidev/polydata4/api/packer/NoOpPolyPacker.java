package com.unidev.polydata4.api.packer;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonFactoryBuilder;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.unidev.polydata4.domain.BasicPoly;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.IOUtils;

import java.io.IOException;
import java.io.InputStream;

/**
 * No Operation poly packer
 */
@Slf4j
public class NoOpPolyPacker implements PolyPacker {

    public byte[] packPoly(BasicPoly poly) throws IOException {
        String value = objectMapper.writeValueAsString(poly);
        return value.getBytes();
    }    @Getter
    @Setter
    private ObjectMapper objectMapper = objectMapper();

    public BasicPoly unPackPoly(InputStream stream) throws IOException {
        return objectMapper.readValue(IOUtils.toByteArray(stream), BasicPoly.class);
    }

    protected ObjectMapper objectMapper() {
        return objectMapper = new ObjectMapper(
                new JsonFactoryBuilder()
                        .configure(JsonFactory.Feature.INTERN_FIELD_NAMES, false)
                        .configure(JsonFactory.Feature.CANONICALIZE_FIELD_NAMES, false)
                        .build()
        );
    }



}
