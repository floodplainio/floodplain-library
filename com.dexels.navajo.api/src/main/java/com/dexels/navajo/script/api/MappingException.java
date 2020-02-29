

/**
 * Title:        Navajo Product Project
 * Description:  This is the official source for the Navajo server
 * Copyright:    Copyright (c) 2002
 * Company:      Dexels BV
 * @author Arjen Schoneveld
 * @version $Id: 9a5ce3d0a382f2f1a093697291571acbecd59dd6 $
 */

package com.dexels.navajo.script.api;


public class MappingException extends Exception {

    /**
	 * 
	 */
	private static final long serialVersionUID = 7107286296359045746L;

	public MappingException(String s) {
        super(s);
    }

	public MappingException(String s, Throwable t) {
        super(s,t);
    }

}
