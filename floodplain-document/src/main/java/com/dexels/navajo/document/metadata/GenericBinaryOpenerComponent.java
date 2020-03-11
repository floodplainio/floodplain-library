package com.dexels.navajo.document.metadata;

import com.dexels.navajo.document.BinaryOpenerFactory;

public class GenericBinaryOpenerComponent extends GenericBinaryOpener {

	public GenericBinaryOpenerComponent() {
		super();
	}
	
	public void activate() {
		BinaryOpenerFactory.setInstance(this);

	}
	
	public void deactivate() {
		BinaryOpenerFactory.setInstance(null);
	}
}
