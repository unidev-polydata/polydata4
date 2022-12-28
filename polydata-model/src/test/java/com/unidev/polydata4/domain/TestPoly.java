package com.unidev.polydata4.domain;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestPoly {

    ObjectMapper objectMapper = new ObjectMapper();

    @Test
    public void mapSerialization() throws JsonProcessingException {
        BasicPoly in = BasicPoly.newPoly("test");
        Map<String, String> qwe = new HashMap<>();
        qwe.put("xxx", "yyy");
        in.put("qwe", qwe);

        String raw = objectMapper.writeValueAsString(in);
        BasicPoly out = objectMapper.readValue(raw, BasicPoly.class);

        Map<String, String> qwe2 = out.fetch("qwe");

        assertEquals("yyy", qwe2.get("xxx"));

        qwe2.put("1", "2");
        out.put("qwe", qwe2);

        String raw2 = objectMapper.writeValueAsString(out);
        BasicPoly out2 = objectMapper.readValue(raw2, BasicPoly.class);

        Map<String, String> qwe3 = out2.fetch("qwe");
        assertEquals("yyy", qwe3.get("xxx"));
        assertEquals("2", qwe3.get("1"));

    }

}
