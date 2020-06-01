package ru.fix.zookeeper.utils;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.fasterxml.jackson.module.kotlin.KotlinModule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class Marshaller {
    private static final Logger logger = LoggerFactory.getLogger(Marshaller.class);

    private static final ObjectMapper mapper = new ObjectMapper()
            .setSerializationInclusion(JsonInclude.Include.NON_NULL)
            .registerModule(new KotlinModule())
            .registerModule(new JavaTimeModule())
            .enable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
            .enable(DeserializationFeature.ACCEPT_SINGLE_VALUE_AS_ARRAY);

    public static String marshall(Object serializedObject) {
        try {
            return mapper.writeValueAsString(serializedObject);
        } catch (JsonProcessingException ex) {
            logger.trace("Failed to marshalling pojo. Object details: {}", serializedObject, ex);
            throw new IllegalStateException(ex);
        }
    }

    public static <T> T unmarshall(String json, Class<T> targetType) throws IOException {
        try {
            return mapper.readValue(json, targetType);
        } catch (IOException ex) {
            logger.trace("Failed to unmarshall json text to type {}. Json: {}", targetType, json, ex);
            throw ex;
        }
    }
}
