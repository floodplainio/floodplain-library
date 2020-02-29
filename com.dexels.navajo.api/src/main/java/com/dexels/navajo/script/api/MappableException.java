

/**
 * Title:        Navajo Product Project
 * Description:  This is the official source for the Navajo server
 * Copyright:    Copyright (c) 2002
 * Company:      Dexels BV
 * @author Arjen Schoneveld
 * @version $Id: 430aaded3d7552f5a7d634aee7eb7cc2097fe760 $
 */

package com.dexels.navajo.script.api;


/**
 * This class is used for throwing Exception from a Mappable object's load() and store() methods.
 */

public class MappableException extends Exception {

    /**
	 * 
	 */
	private static final long serialVersionUID = -437908771360471541L;

	public MappableException() {
        super();
    }

    public MappableException(String s) {
        super(s);
    }

    public MappableException(String s, Throwable cause) {
        super(s,cause);
    }

}
