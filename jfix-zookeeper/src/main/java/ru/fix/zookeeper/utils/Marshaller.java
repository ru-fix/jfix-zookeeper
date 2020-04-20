package ru.fix.zookeeper.utils;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.fasterxml.jackson.module.kotlin.KotlinModule;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;

@Slf4j
public class Marshaller {

    private static final ObjectMapper mapper = new ObjectMapper()
            .setSerializationInclusion(JsonInclude.Include.NON_NULL)
            .registerModule(new KotlinModule())
            .registerModule(new JavaTimeModule())
            .enable(DeserializationFeature.ACCEPT_SINGLE_VALUE_AS_ARRAY);

    public static String marshall(Object serializedObject) throws JsonProcessingException {
        try {
            return mapper.writeValueAsString(serializedObject);
        } catch (JsonProcessingException ex) {
            log.trace("Failed to marshalling pojo. Object details: {}", serializedObject, ex);
            throw ex;
        }
    }

    public static <T> T unmarshall(String json, Class<T> targetType) throws IOException {
        try {
            return mapper.readValue(json, targetType);
        } catch (IOException ex) {
            log.trace("Failed to unmarshall json text to type {}. Json: {}", targetType, json, ex);
            throw ex;
        }
    }
}
