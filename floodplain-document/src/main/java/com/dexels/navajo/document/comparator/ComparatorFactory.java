package com.dexels.navajo.document.comparator;

import com.dexels.navajo.document.Message;

import java.util.Comparator;

public interface ComparatorFactory {
	public Comparator<Message> createComparator();
	public String getName();
}
