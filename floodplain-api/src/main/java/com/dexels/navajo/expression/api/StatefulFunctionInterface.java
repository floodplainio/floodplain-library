package com.dexels.navajo.expression.api;

/**
 * Title:
 * Description:
 * Copyright:    Copyright (c) 2001
 * Company:
 * @author
 * @version 1.0
 */

import com.dexels.navajo.document.Message;
import com.dexels.navajo.document.Navajo;
import com.dexels.navajo.script.api.Access;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("rawtypes")
public abstract class StatefulFunctionInterface extends FunctionInterface {

	private Access access;

	protected Navajo inMessage = null;
	protected Message currentMessage = null;

	
	private static final Logger logger = LoggerFactory.getLogger(StatefulFunctionInterface.class);

	
	public void setCurrentMessage(Message currentMessage) {
		this.currentMessage = currentMessage;
	}




	public StatefulFunctionInterface() {
	}

	public boolean isPure() {
		return false;
	}


	protected final Navajo getNavajo() {
		return this.inMessage;
	}

	protected final Message getCurrentMessage() {
		return this.currentMessage;
	}


	public void setInMessage(Navajo inMessage) {
		this.inMessage = inMessage;
	}

	public void setAccess(Access access) {
		this.access = access;
	}

	public Access getAccess() {
		return this.access;
	}

	public String getInstance() {
		if (this.access == null) {
			return null;
		} else {
			return this.access.getTenant();
		}
	}


}
