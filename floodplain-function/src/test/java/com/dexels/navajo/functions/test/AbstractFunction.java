package com.dexels.navajo.functions.test;

import com.dexels.navajo.functions.util.FunctionFactoryFactory;
import com.dexels.navajo.functions.util.FunctionFactoryInterface;
import org.junit.Before;

public abstract class AbstractFunction  {

	protected FunctionFactoryInterface fff;
	protected ClassLoader cl;
	
	@Before
	public void setUp() throws Exception {
		fff = FunctionFactoryFactory.getInstance();
		cl = getClass().getClassLoader();
	}

}
