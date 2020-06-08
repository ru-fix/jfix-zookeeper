package ru.fix.zookeeper.utils;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.fix.zookeeper.lock.LockData;

import java.io.IOException;
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

    private final String lockDataJson = "" +
            "{\n" +
            "  \"uuid\": \"1cf7407a-9f15-11e9-a2a3-2a2ae2dbcce4\",\n" +
            "  \"expirationTimestamp\": \"2020-05-26T07:01:25.589Z\",\n" +
            "  \"ip\": \"93.184.216.34\",\n" +
            "  \"hostname\": \"example.org\"\n" +
            "}";

    @Test
    public void marshallLockData() throws IOException {
        String json = Marshaller.marshall(lockData);
        assertEquals(json.replaceAll("\\s+", ""), lockDataJson.replaceAll("\\s+", ""));
        logger.trace("Marshalling successfull.\n{}", json);
    }

    @Test
    public void unmarshallLockData() throws IOException {
        String json = lockDataJson;
        LockData lockData = Marshaller.unmarshall(json, LockData.class);
        assertNotNull(lockData);
        assertEquals(lockData, this.lockData);
        logger.trace("Unmarshalling successfull.\n{}", lockData);
    }
}
