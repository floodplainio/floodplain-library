package com.dexels.navajo.document.test;

import com.dexels.navajo.document.*;
import org.junit.Test;


public class TestOperation {

	@Test
	public void testOperation() {
		
		NavajoFactory f = NavajoFactory.getInstance();
		
		
		Navajo n = f.createNavajo();
		Message msg = f.createMessage(n, "__Mongo__");
		
		
		Operation o = f.createOperation(n, "PUT", "vla/ProcessInsertPerson", null, "Person", null);
		n.addOperation(o);
		o.setExtraMessage(msg);
		Method m = f.createMethod(n, "vla/ProcessUpdatePerson", null);
		m.addRequired("Apenoot");
		n.addMethod(m);
		
		
		n.write(System.err);
		
	}
}
