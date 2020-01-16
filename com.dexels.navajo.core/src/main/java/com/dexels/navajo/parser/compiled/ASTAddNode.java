/* Generated By:JJTree&JavaCC: Do not edit this line. ASTAddNode.java */
package com.dexels.navajo.parser.compiled;


import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;

import com.dexels.navajo.document.Operand;
import com.dexels.navajo.expression.api.ContextExpression;
import com.dexels.navajo.expression.api.FunctionClassification;
import com.dexels.navajo.expression.api.TMLExpressionException;
import com.dexels.navajo.parser.Utils;

@SuppressWarnings({"unchecked","rawtypes"})
final class ASTAddNode extends SimpleNode {

    ASTAddNode(int id) {
        super(id);
    }


	@Override
	public ContextExpression interpretToLambda(List<String> problems, String expression, Function<String, FunctionClassification> functionClassifier, Function<String,Optional<Node>> mapResolver) {
		return untypedLazyBiFunction(problems,expression, (a,b)->interpret(a, b,expression),functionClassifier,mapResolver);
	}
	
	private final Operand interpret(Operand ao,Operand bo, String expression) {
		Object a = ao.value;
		Object b = bo.value;
        if (!(a instanceof ArrayList || b instanceof ArrayList)) {
            return Operand.ofDynamic(Utils.add(a, b, expression));
        } else if ((a instanceof List) && !(b instanceof List)) {
        	List list = (List) a;
        	List result = new ArrayList();

            for (int i = 0; i < list.size(); i++) {
                Object val = list.get(i);
                Object rel = Utils.add(val, b, expression);

                result.add(rel);
            }
            return Operand.ofList(result);
        } else if ((b instanceof ArrayList) && !(a instanceof ArrayList)) {
            ArrayList list = (ArrayList) b;
            ArrayList result = new ArrayList();

            for (int i = 0; i < list.size(); i++) {
                Object val = list.get(i);
                Object rel = Utils.add(a, val, expression);

                result.add(rel);
            }
            return Operand.ofList(result);
        } else if (a instanceof ArrayList && b instanceof ArrayList) {
            ArrayList list1 = (ArrayList) a;
            ArrayList list2 = (ArrayList) b;

            if (list1.size() != list2.size())
                throw new TMLExpressionException("Can only add lists of equals length. Lengths found: "+list1.size()+" and"+list2.size()+" expression: "+expression);
            ArrayList result = new ArrayList();

            for (int i = 0; i < list1.size(); i++) {
                Object val1 = list1.get(i);
                Object val2 = list2.get(i);
                Object rel = Utils.add(val1, val2,expression);

                result.add(rel);
            }
            return Operand.ofList(result);
        } else
            throw new TMLExpressionException("Unknown type");
    }

}