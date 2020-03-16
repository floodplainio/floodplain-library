package com.dexels.navajo.document.comparatormanager;

import com.dexels.navajo.document.Message;

import java.util.Comparator;

public interface ComparatorManager {
	public Comparator<Message> getComparator(String name);
}
