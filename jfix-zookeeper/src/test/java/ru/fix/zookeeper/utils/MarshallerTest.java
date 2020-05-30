package ru.fix.zookeeper.utils;

import com.fasterxml.jackson.core.type.TypeReference;
import kotlin.text.Charsets;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.fix.zookeeper.lock.LockData;

import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Instant;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class MarshallerTest {

    private final Logger logger = LoggerFactory.getLogger(MarshallerTest.class);

    private final LockData lockData = new LockData(
            "93.184.216.34",
            "example.org",
            "1cf7407a-9f15-11e9-a2a3-2a2ae2dbcce4",
            Instant.parse("2020-05-26T07:01:25.589Z")
    );

    @Test
    public void marshallLockData() throws IOException {
        String json = Marshaller.marshall(lockData);
        assertEquals(json, getJsonFromResource("lockData.json"));
        logger.trace("Marshalling successfull.\n{}", json);
    }

    @Test
    public void unmarshallLockData() throws IOException {
        String json = getJsonFromResource("lockData.json");
        LockData lockData = Marshaller.unmarshall(json, new TypeReference<>() {});
        assertNotNull(lockData);
        assertEquals(lockData, this.lockData);
        logger.trace("Unmarshalling successfull.\n{}", lockData);
    }

    private String getJsonFromResource(String name) throws IOException {
        URL url = getClass().getClassLoader().getResource(name);
        assertNotNull(url);
        String json = "";
        try {
            json = Files.readString(Paths.get(url.toURI()), Charsets.UTF_8);
        } catch (URISyntaxException e) {
            logger.error("Invalid URL: {}", url, e);
        }
        return json.replaceAll("\\s", "");
    }
}