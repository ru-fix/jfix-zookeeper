package ru.fix.zookeeper.lock;

import org.apache.commons.io.IOUtils;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URL;
import java.nio.charset.StandardCharsets;

public class MarshallerTest {

    private final Logger log = LoggerFactory.getLogger(MarshallerTest.class);

    private final LockData lockData = new LockData("93.184.216.34",
                                                   "example.org",
                                                   "2336799_DOMAIN_COM-VRSN",
                                                   "1cf7407a-9f15-11e9-a2a3-2a2ae2dbcce4",
                                                   1562324887282L);

    @Test
    public void marshallLockData() throws IOException {
        String json = Marshaller.marshall(lockData);
        Assert.assertEquals(json, getJsonFromResource("lockData.json"));
        log.trace("Marshalling successfull.\n{}", json);
    }

    @Test
    public void unmarshallLockData() throws IOException {
        String json = getJsonFromResource("lockData.json");
        LockData lockData = Marshaller.unmarshall(json, LockData.class);
        Assert.assertNotNull(lockData);
        Assert.assertEquals(lockData, this.lockData);
        log.trace("Unmarshalling successfull.\n{}", lockData);
    }

    private String getJsonFromResource(String name) throws IOException {
        URL url = getClass().getClassLoader().getResource(name);
        Assert.assertNotNull(url);
        String json = IOUtils.toString(url, StandardCharsets.UTF_8);
        return json.replaceAll("\\s", "");
    }
}
