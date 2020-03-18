package com.dexels.navajo.functions;


import com.dexels.navajo.document.Message;
import com.dexels.navajo.document.types.Binary;
import com.dexels.navajo.expression.api.FunctionInterface;
import com.dexels.navajo.expression.api.TMLExpressionException;

import java.util.List;


/**
 * Title:        Navajo
 * Description:
 * Copyright:    Copyright (c) 2001
 * Company:      Dexels
 * @author Arjen Schoneveld en Martin Bergman
 * @version $Id$
 */

public final class Size extends FunctionInterface {

    public Size() {}

    @Override
	@SuppressWarnings("rawtypes")
	public final Object evaluate() throws com.dexels.navajo.expression.api.TMLExpressionException {
    	if(this.getOperands().size()==0) {
    		return Integer.valueOf(0);
    	}
    	if(this.getOperands().size()==1 && this.getOperands().get(0)==null) {
    		return Integer.valueOf(0);
    	}    	
    	
    	Object arg = this.getOperands().get(0);

        //System.out.println("IN SIZE(), ARG = " + arg);
        if (arg == null) {
            throw new TMLExpressionException("Argument expected for Size() function.");
        }
        else if (arg instanceof java.lang.String) {
            return Integer.valueOf(((String) arg).length());
        }
        else if (arg instanceof Binary) {
        	return Integer.valueOf( (int) ((Binary) arg).getLength());
        } 
        else if (arg instanceof Message) {
        	return Integer.valueOf( ((Message) arg).getArraySize());
        } 
        else if (arg instanceof Object[]) {
          	return Integer.valueOf( ((Object[]) arg).length);
          } 
          
        else if (!(arg instanceof List)) {
            throw new TMLExpressionException("Expected list argument for size() function.");
        }
        
        List list = (List) arg;

        return Integer.valueOf(list.size());
    }

    @Override
	public String usage() {
        return "Size(list | arraymessage | array)";
    }

    @Override
	public String remarks() {
        return "This function return the size of a list argument, the length of an array, or the size of an array message.";
    }

}
