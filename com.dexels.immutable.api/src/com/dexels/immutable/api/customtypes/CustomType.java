package com.dexels.immutable.api.customtypes;

import java.io.Serializable;

/**
 * <p>
 * Title:
 * </p>
 * <p>
 * Description:
 * </p>
 * <p>
 * Copyright: Copyright (c) 2003
 * </p>
 * <p>
 * Company:
 * </p>
 * 
 * @author not attributable
 * @version 1.0
 */

public abstract class CustomType implements Serializable {

    /**
     * 
     */
    private static final long serialVersionUID = 7902325236842496511L;

    public CustomType() {

    }

    public abstract boolean isEmpty();

    public abstract String toString();

}
