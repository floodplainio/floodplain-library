package com.dexels.navajo.script.api;

/**
 * <p>Title: Navajo Product Project</p>
 * <p>Description: This is the official source for the Navajo server</p>
 * <p>Copyright: Copyright (c) 2002</p>
 * <p>Company: Dexels BV</p>
 * @author Arjen Schoneveld
 * @version 1.0
 */

import java.io.Serializable;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;


@SuppressWarnings("unchecked")
public final class MappableTreeNode implements Serializable {

    /**
	 * 
	 */
    private static final long serialVersionUID = 6880152616096374576L;

    public MappableTreeNode parent = null;
    public transient Object myObject = null;
    public String name = "";
    public long starttime;

    // HashMap to cache method references.
    private transient Map<String, Method> methods;
    private int id = 0;
    public MappableTreeNode(MappableTreeNode parent, Object o) {
        this( parent, o, false);
    }

    public MappableTreeNode(MappableTreeNode parent, Object o, boolean isArrayElement) {
        this.parent = parent;
        this.myObject = o;
        methods = new HashMap<>();
        starttime = System.currentTimeMillis();
        if (parent == null) {
            id = 0;
        } else {
            id = parent.getId() + 1;
        }

    }



    public int getId() {
        return id;
    }


}
