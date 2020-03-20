/* Generated By:JJTree&JavaCC: Do not edit this line. ASTOptionNode.java */
package com.dexels.navajo.parser.compiled;

import com.dexels.immutable.api.ImmutableMessage;
import com.dexels.immutable.api.ImmutableMessage.ValueType;
import com.dexels.navajo.document.operand.Operand;
import com.dexels.navajo.document.Property;
import com.dexels.navajo.expression.api.ContextExpression;
import com.dexels.navajo.expression.api.FunctionClassification;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Optional;
import java.util.function.Function;

final class ASTOptionNode extends SimpleNode {

    private String option = "";
    
	private static final Logger logger = LoggerFactory.getLogger(ASTOptionNode.class);


    ASTOptionNode(int id) {
        super(id);
    }

	@Override
	public ContextExpression interpretToLambda(List<String> problems, String expression, Function<String, FunctionClassification> functionClassifier, Function<String,Optional<Node>> mapResolver) {
		return new ContextExpression() {
			
			@Override
			public boolean isLiteral() {
				return false;
			}
			
			@Override
			public Operand apply(Optional<ImmutableMessage> immutableMessage, Optional<ImmutableMessage> paramMessage) {
				return Operand.ofString(ASTOptionNode.this.option);
			}

			@Override
			public Optional<ValueType> returnType() {
				logger.warn("Sketchy type resolution of option. Assuming integer?!");
				return Optional.of(ValueType.INTEGER);
			}
			
			@Override
			public String expression() {
				return expression;
			}
		};
	}
}
