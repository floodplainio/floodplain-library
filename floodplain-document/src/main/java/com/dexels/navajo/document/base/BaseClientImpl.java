package com.dexels.navajo.document.base;

import com.dexels.navajo.document.Navajo;

import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class BaseClientImpl extends BaseNode {

	private static final long serialVersionUID = 6391221035914936805L;
	private final Map<String,String> myAttributes = new HashMap<>();
  
  public BaseClientImpl(Navajo n) {
    super(n);
  }

    @Override
	public Map<String,String> getAttributes() {
        return myAttributes;
    }

    @Override
	public List<BaseNode> getChildren() {
        return null;
    }

    @Override
	public String getTagName() {
        return "client";
    }

    public String getHost() {
        return myAttributes.get("host");
    }
    public String getAddress() {
        return myAttributes.get("address");
    }
   
    public void setHost(String host) {
        myAttributes.put("host", host);
    }
    public void setAddress(String host) {
        myAttributes.put("host", host);
    }
}

// EOF $RCSfile$ //
