/* Generated By:JJTree&JavaCC: Do not edit this line. ASTFloatConstantNode.java */
package com.dexels.navajo.parser.compiled;

import com.dexels.immutable.api.ImmutableMessage;
import com.dexels.navajo.document.operand.Operand;
import com.dexels.navajo.expression.api.ContextExpression;
import com.dexels.navajo.expression.api.FunctionClassification;

import java.util.List;
import java.util.Optional;
import java.util.function.Function;

final class ASTFloatConstantNode extends SimpleNode {

    double val;

    ASTFloatConstantNode(int id) {
        super(id);
    }

	@Override
	public ContextExpression interpretToLambda(List<String> problems, String expression, Function<String, FunctionClassification> functionClassifier, Function<String,Optional<Node>> mapResolver) {
		return new ContextExpression() {
			
			@Override
			public boolean isLiteral() {
				return true;
			}
			
			@Override
			public Operand apply(Optional<ImmutableMessage> immutableMessage, Optional<ImmutableMessage> paramMessage) {
		        return Operand.ofFloat(val);
			}

			@Override
			public Optional<ImmutableMessage.ValueType> returnType() {
				return Optional.of(ImmutableMessage.ValueType.FLOAT);
			}
			
			@Override
			public String expression() {
				return expression;
			}
		};
	}

}
