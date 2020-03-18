
package com.dexels.navajo.document.base;

import com.dexels.navajo.document.Required;

/**
 * @author arjen
 *
 * Window - Preferences - Java - Code Style - Code Templates
 */
public class BaseRequiredImpl implements Required {

	private String message;
	private String filter;
	
	/* (non-Javadoc)
	 * @see com.dexels.navajo.document.Required#setMethod(java.lang.String)
	 */
	@Override
	public void setMessage(String s) {
		message = s;
	}

	/* (non-Javadoc)
	 * @see com.dexels.navajo.document.Required#getMethod()
	 */
	@Override
	public String getMessage() {
		return message;
	}

	/* (non-Javadoc)
	 * @see com.dexels.navajo.document.Required#setFilter(java.lang.String)
	 */
	@Override
	public void setFilter(String f) {
		filter = f;
	}

	/* (non-Javadoc)
	 * @see com.dexels.navajo.document.Required#getFilter()
	 */
	@Override
	public String getFilter() {
		return filter;
	}
	
}
