package com.unidev.polydata4.domain;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

import java.util.Map;
import java.util.Optional;

@AllArgsConstructor
public class DataTransform {

    @Getter
    @Setter
    private final ObjectMapper objectMapper;

    public Optional<Map> toMap(Object object) {
        try {
            return Optional.ofNullable(objectMapper.readValue(objectMapper.writeValueAsBytes(object), Map.class));
        } catch (Exception e) {
            return Optional.empty();
        }
    }

    public <T> Optional<T> toObject(Object object, Class<T> clazz) {
        try {
            return Optional.ofNullable(objectMapper.readValue(objectMapper.writeValueAsBytes(object), clazz));
        } catch (Exception e) {
            return Optional.empty();
        }
    }

}
