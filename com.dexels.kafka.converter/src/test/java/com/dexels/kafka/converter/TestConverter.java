package com.dexels.kafka.converter;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;

import org.apache.kafka.connect.data.SchemaAndValue;
import org.junit.Before;
import org.junit.Test;

public class TestConverter {
	
	@Before
	public void setup() {
//		ReplicationFactory.setInstance(new FallbackReplicationMessageParser());
		
	}
	@Test
	public void testConverter() throws IOException {
//		ReplicationMessage msg =  ReplicationFactory.getInstance().parseStream(TestConverter.class.getClassLoader().getResourceAsStream("example.json"));
		ReplicationMessageConverter converter = new ReplicationMessageConverter();
		converter.configure(Collections.emptyMap(), false);
		SchemaAndValue sav = converter.toConnectData("any", readStream(TestConverter.class.getClassLoader().getResourceAsStream("example.json")));
//		byte[] data = converter.fromConnectData("any", null,msg);
		;
		System.err.println("Result: "+sav.value());
				
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