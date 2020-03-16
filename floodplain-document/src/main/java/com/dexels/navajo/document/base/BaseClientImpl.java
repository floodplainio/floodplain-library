package com.dexels.navajo.document.base;

import com.dexels.navajo.document.Navajo;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * <p>Title: ShellApplet</p>
 * <p>Description: </p>
 * <p>Part of the Navajo mini client, based on the NanoXML parser</p>
 * <p>Copyright: Copyright (c) 2002</p>
 * <p>Company: Dexels </p>
 * <p>$Id$</p>
 * @author Frank Lyaruu
 * @version $Revision$
 */
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
