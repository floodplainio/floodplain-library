package com.dexels.navajo.functions.test;

import com.dexels.navajo.document.operand.Binary;
import com.dexels.navajo.functions.security.Security;
import org.junit.Assert;
import org.junit.Test;

import java.io.*;

public class TestEncryptBinary {

	@Test
	public void testEncryptBinary() throws Exception {
		
		Security s = new Security("A");
		File f = new File("test");
		FileOutputStream fos = new FileOutputStream(f);
		fos.write("Content".getBytes());
		fos.close();
		
		Binary b = new Binary(new FileInputStream(f));
		
		String enc = s.encrypt(b);
		
		System.err.println("enc: " + enc);
		
		Binary b2 = s.decryptBinary(enc);
		
		BufferedReader is = new BufferedReader(new InputStreamReader(b2.getDataAsStream(),"UTF-8"));
		String l = is.readLine();
		is.close();
		
		Assert.assertEquals("Content", l);
		
		f.delete();
	}
}
