package io.floodplain.kafka.converter;

import org.apache.kafka.connect.data.SchemaAndValue;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;

public class TestConverter {

    private final static Logger logger = LoggerFactory.getLogger(TestConverter.class);

    @Before
    public void setup() {
//		ReplicationFactory.setInstance(new FallbackReplicationMessageParser());

    }

    @Test
    public void testConverter() throws IOException {
        ReplicationMessageConverter converter = new ReplicationMessageConverter();
        converter.configure(Collections.emptyMap(), false);
        SchemaAndValue sav = converter.toConnectData("any", readStream(TestConverter.class.getClassLoader().getResourceAsStream("example.json")));
        logger.info("Result: {}",sav.value());
    }

    public byte[] readStream(InputStream is) throws IOException {

        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        int nRead;
        byte[] data = new byte[1024];
        while ((nRead = is.read(data, 0, data.length)) != -1) {
            buffer.write(data, 0, nRead);
        }

        buffer.flush();
        return buffer.toByteArray();
    }
}
