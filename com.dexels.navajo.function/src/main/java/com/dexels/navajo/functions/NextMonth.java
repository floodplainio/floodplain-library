package com.dexels.navajo.functions;


import java.util.Calendar;

import com.dexels.navajo.expression.api.FunctionInterface;


/**
 * Title:        Navajo
 * Description:
 * Copyright:    Copyright (c) 2001
 * Company:      Dexels
 * @author Arjen Schoneveld en Martin Bergman
 * @version $Id: 4d4dbef31958cfa4721f2a1b2dc655552167d592 $
 */

public final class NextMonth extends FunctionInterface {

    public NextMonth() {}

    @Override
	public String remarks() {
        return "Get the next month as a date object, where supplied integer is an offset for the current year.";
    }

    @Override
	public String usage() {
        return "NextMonth(Integer)";
    }

    @Override
	public final Object evaluate() throws com.dexels.navajo.expression.api.TMLExpressionException {

        java.util.Date datum = new java.util.Date();
        Calendar c = Calendar.getInstance();

        c.setTime(datum);

        Integer arg = (Integer) this.getOperands().get(0);
        int offset = arg.intValue();
        c.set(c.get(Calendar.YEAR) + offset, c.get(Calendar.MONTH) + 1, 1);

//        java.text.SimpleDateFormat formatter = new java.text.SimpleDateFormat("yyyy-MM-dd");

        // return formatter.format(c.getTime());
        return c.getTime();
    }
}
