package com.dexels.navajo.functions.security;

import com.dexels.navajo.document.operand.Binary;

import javax.crypto.Cipher;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.Charset;
import java.security.Key;
import java.util.Base64;

public class Security {
	private static final String KEY_ALGO = "AES";
	private static final String ALGO = "AES/ECB/PKCS5Padding";
	private byte[] keyValue = null;

	public Security(String secret) {
		// Transform secret to 128 bit key.
		if ( secret.length() > 16 ) {
			secret = secret.substring(0, 16);
		} else if ( secret.length() < 16 ) {
			int missing = 16 - secret.length();
			for ( int i = 0; i < missing; i++ ) {
				secret = secret + (i % 10);
			}
		}
		keyValue = secret.getBytes(Charset.forName("US-ASCII"));
	}
	
	public String encrypt(Binary data) throws Exception {
		
		Key key = generateKey();
		Cipher c = Cipher.getInstance(ALGO);
		c.init(Cipher.ENCRYPT_MODE, key);
		byte[] encVal = c.doFinal(data.getData());
		
		String encryptedValue = new String(Base64.getEncoder().encode(encVal)); // .encode(encVal);
		return encryptedValue;
		
	}
	
	public String encrypt(String Data) throws Exception {
		Key key = generateKey();
		Cipher c = Cipher.getInstance(ALGO);
		c.init(Cipher.ENCRYPT_MODE, key);
		byte[] encVal = c.doFinal(Data.getBytes());
		
		String encryptedValue = new String(Base64.getEncoder().encode(encVal));
		return encryptedValue;
	}

	public Binary decryptBinary(String encryptedData) throws Exception {
		Key key = generateKey();
		Cipher c = Cipher.getInstance(ALGO);
		c.init(Cipher.DECRYPT_MODE, key);
		byte[] decordedValue = Base64.getDecoder().decode(encryptedData);
		byte[] decValue = c.doFinal(decordedValue);
		Binary b = new Binary(decValue);
		return b;
	}
	
	public String decrypt(String encryptedData) throws Exception {
		Key key = generateKey();
		Cipher c = Cipher.getInstance(ALGO);
		c.init(Cipher.DECRYPT_MODE, key);
//		byte[] decordedValue = Base64.decode(encryptedData);
		byte[] decordedValue = Base64.getDecoder().decode(encryptedData);
		byte[] decValue = c.doFinal(decordedValue);
		String decryptedValue = new String(decValue);
		return decryptedValue;
	}
	
	public Key generateKey() throws Exception {
		Key key = new SecretKeySpec(keyValue, KEY_ALGO);
		return key;
	}

}
