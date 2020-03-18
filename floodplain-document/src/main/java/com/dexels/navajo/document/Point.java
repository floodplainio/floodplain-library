package com.dexels.navajo.document;

public interface Point {

   public void setValue(String s);

    public void setValue(int position, String s);

    public String getValue(int position);

    public int getSize();
}