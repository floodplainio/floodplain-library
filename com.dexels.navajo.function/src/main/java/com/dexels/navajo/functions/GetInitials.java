package com.dexels.navajo.functions;


import java.util.StringTokenizer;

import com.dexels.navajo.expression.api.FunctionInterface;
import com.dexels.navajo.expression.api.TMLExpressionException;


/**
 * Title:        Navajo
 * Description:
 * Copyright:    Copyright (c) 2001
 * Company:      Dexels
 * @author Arjen Schoneveld en Martin Bergman
 * @version $Id: dfa07b7e846e3587bba6e0f2f368a0974aab7a6a $
 */

public final class GetInitials extends FunctionInterface {

    public GetInitials() {// Hallo
    }

    @Override
	public final Object evaluate() throws com.dexels.navajo.expression.api.TMLExpressionException {
        Object a = this.getOperands().get(0);
        String text;
        try{
          text = (String) a;
        }catch(ClassCastException e){
          text = a.toString();
        }
        if(text == null){
          throw new TMLExpressionException("GetInitials(): failed because the input was null");
        }
        StringTokenizer tokens = new StringTokenizer(text, " ");
        String result = "";

        while(tokens.hasMoreTokens()){
          String name = tokens.nextToken();
          result = result + name.substring(0,1)+".";
        }
        return result.trim();
    }

    @Override
	public String usage() {
        return "GetInitials(string)";
    }

    @Override
	public String remarks() {
        return "This function returns the initials of a specified string field . Eg. GetInitials('aap noot mies') results in 'a n m'";
    }
}
