
package com.dexels.navajo.document;

import java.util.Random;

public final class Guid {

	static final Random rand = new Random(System.currentTimeMillis() + 
			 Math.abs( (System.class.hashCode() == Integer.MIN_VALUE ? 3213232 : System.class.hashCode() )));

	private Guid() {
		// no instance
	}
	public static final String create() {
		return Math.abs(rand.nextLong() * System.currentTimeMillis()) + "";
	}
	
	
}
