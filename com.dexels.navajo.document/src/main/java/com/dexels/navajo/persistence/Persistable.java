package com.dexels.navajo.persistence;


/**
 * Title:        Navajo Product Project
 * Description:  This is the official source for the Navajo server
 * Copyright:    Copyright (c) 2002
 * Company:      Dexels BV
 * @author Arjen Schoneveld
 * @version $Id: 88e0e1a1c65f41d0ab15a1490ea9fa9137c84ad2 $
 */

/**
 * The Navajo class should implement this interface.
 */
public interface Persistable extends java.io.Serializable {

    /**
     * Generate a unique key.
     */
    public String persistenceKey();

}
