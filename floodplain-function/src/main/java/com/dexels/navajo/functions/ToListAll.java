package com.dexels.navajo.functions;

import com.dexels.navajo.expression.api.FunctionInterface;

import java.util.ArrayList;
import java.util.List;

public final class ToListAll extends FunctionInterface {
    
    @SuppressWarnings("unchecked")
    @Override
    public final Object evaluate() throws com.dexels.navajo.expression.api.TMLExpressionException {
        List<Object> result = new ArrayList<>();
        
        for (int i=0;i<getOperands().size();i++) {
            Object elem = this.getOperands().get(i);
            if (elem == null) continue;
            
            if (elem instanceof List) {
                List<Object> listElem = (List<Object>) elem;
                result.addAll(listElem);
            } else {
                result.add(elem);

            }
        }
        return result;


    }

    @Override
    public String usage() {
        return "ToList(elems)";
    }

    @Override
    public String remarks() {
        return "Converts all paremeters to a new list";
    }

}
