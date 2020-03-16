package com.dexels.navajo.document.comparator.impl;

import com.dexels.navajo.document.Message;
import com.dexels.navajo.document.comparator.ComparatorFactory;

import java.util.Comparator;

public class PropertyCountComparatorFactory implements ComparatorFactory {

	@Override
	public Comparator<Message> createComparator() {
		return new PropertyCountComparator();
	}

	@Override
	public String getName() {
		return PropertyCountComparator.class.getName();
	}
}
