package com.dexels.navajo.reactive.api;

import com.dexels.immutable.api.ImmutableMessage;
import com.dexels.navajo.document.Message;
import com.dexels.navajo.document.Navajo;
import com.dexels.navajo.document.Operand;
import com.dexels.navajo.document.Selection;
import com.dexels.navajo.document.stream.api.StreamScriptContext;
import com.dexels.navajo.expression.api.ContextExpression;
import com.dexels.navajo.expression.api.TMLExpressionException;
import com.dexels.navajo.expression.api.TipiLink;
import com.dexels.navajo.script.api.Access;
import com.dexels.navajo.script.api.MappableTreeNode;

import java.util.*;

public class ReactiveParameters {
	
	public final Map<String, ContextExpression> named;
	public final List<ContextExpression> unnamed;
	public final Map<String, ContextExpression> namedState;
	private final ParameterValidator validator;

//	Map<String, ContextExpression> namedParams, List<ContextExpression> unnamedParams
	private ReactiveParameters(ParameterValidator validator, Map<String,ContextExpression> namedParameters,List<ContextExpression> unnamedParameters, Map<String,ContextExpression> namedState) {
		this.validator = validator;
		this.named = namedParameters;
		this.unnamed = unnamedParameters;
		this.namedState = namedState;
	}
	
	public ReactiveResolvedParameters resolve(StreamScriptContext context, Optional<ImmutableMessage> currentMessage,ImmutableMessage paramMessage, ParameterValidator metadata) {
		return new ReactiveResolvedParameters(context, named,unnamed,namedState, currentMessage, paramMessage, validator);
	}
	public ReactiveResolvedParameters resolveNamed(StreamScriptContext context, Optional<ImmutableMessage> currentMessage,ImmutableMessage paramMessage, ParameterValidator metadata) {
		return new ReactiveResolvedParameters(context, named,Collections.emptyList(),namedState, currentMessage, paramMessage, validator);
	}

	public ReactiveResolvedParameters resolveUnnamed(StreamScriptContext context, Optional<ImmutableMessage> currentMessage,ImmutableMessage paramMessage, ParameterValidator metadata) {
		return new ReactiveResolvedParameters(context, Collections.emptyMap(),unnamed, namedState,currentMessage, paramMessage, validator);
	}

	public ReactiveParameters withConstant(String key, Object value, String type) {
		return withExpression(key, constantExpression( Operand.ofCustom(value, type)));
	}

	private ContextExpression constantExpression(final Operand value) {
		return new ContextExpression() {

			@Override
			public Operand apply(Navajo doc, Message parentMsg, Message parentParamMsg, Selection parentSel,
					MappableTreeNode mapNode, TipiLink tipiLink, Access access,
					Optional<ImmutableMessage> immutableMessage, Optional<ImmutableMessage> paramMessage)
					throws TMLExpressionException {
				return value;
			}

			@Override
			public boolean isLiteral() {
				return true;
			}

			@Override
			public Optional<String> returnType() {
				return Optional.of(value.type);
			}

			@Override
			public String expression() {
				return "";
			}};
	}
	public ReactiveParameters withConstant(Operand constant) {
		return withExpression(constantExpression(constant));
	}

	
	public ReactiveParameters withExpression(ContextExpression expression) {
		List<ContextExpression> list = new ArrayList<ContextExpression>(this.unnamed);
		list.add(expression);
		return new ReactiveParameters(validator, this.named, list,this.namedState);
	}

	public ReactiveParameters withExpression(String key, ContextExpression expression) {
		Map<String,ContextExpression> extended = new HashMap<>(named);
		extended.put(key,expression);
		return with(key,extended);
	}

	public ReactiveParameters with(String key, Map<String,ContextExpression> namedParameters) {
		Map<String,ContextExpression> extended = new HashMap<>(named);
		extended.putAll(namedParameters);
		return ReactiveParameters.of(validator, extended,this.unnamed,this.namedState);
	}
	
	public static ReactiveParameters of(ParameterValidator validator, Map<String,ContextExpression> namedParameters,List<ContextExpression> unnamedParameters, Map<String,ContextExpression> namedState) {
		return new ReactiveParameters(validator,namedParameters,unnamedParameters,namedState);
	}

	public static ReactiveParameters empty(ParameterValidator validator) {
		return ReactiveParameters.of(validator,Collections.emptyMap(),Collections.emptyList(),Collections.emptyMap());
	}

}
