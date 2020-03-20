package com.dexels.navajo.parser.compiled.api;

import com.dexels.immutable.api.ImmutableMessage;
import com.dexels.immutable.api.ImmutableMessage.ValueType;
import com.dexels.navajo.document.operand.Operand;
import com.dexels.navajo.document.stream.ReactiveParseProblem;
import com.dexels.navajo.expression.api.ContextExpression;
import com.dexels.navajo.parser.compiled.Node;
import com.dexels.navajo.reactive.api.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class ReactiveParseItem implements ContextExpression {


	private final String name;
	private final Reactive.ReactiveItemType type;
	private final Map<String, ContextExpression> namedParams;
	private final List<ContextExpression> unnamedParams;
	private final Map<String, ContextExpression> namedState;
	private final String expression;
	private final Node node;
	
	public ReactiveParseItem(String name, Reactive.ReactiveItemType type, Map<String,ContextExpression> namedParams, List<ContextExpression> unnamedParams, Map<String,ContextExpression> namedState, String expression, Node node) {
		this.name = name;
		this.type = type;
		this.namedParams = namedParams;
		this.unnamedParams = unnamedParams;
		this.namedState = namedState;
		this.expression = expression;
		this.node = node;
	}

	@Override
	public Operand apply(Optional<ImmutableMessage> immutableMessage,
			Optional<ImmutableMessage> paramMessage) {
		return materializeReactive();
	}

	private Operand materializeReactive() {
		switch (type) {
		case REACTIVE_SOURCE:
			ReactiveSourceFactory sourceFactory = Reactive.finderInstance().getSourceFactory(name);
			if(sourceFactory==null) {
				throw new ReactiveParseException("No source found named: "+name);
			}
			return new Operand(sourceFactory.build(ReactiveParameters.of(sourceFactory, namedParams, unnamedParams,namedState)), ValueType.MAPPER);
		case REACTIVE_HEADER:
			break;
		case REACTIVE_MAPPER:
			ReactiveMerger mergerFactory = Reactive.finderInstance().getMergerFactory(name);
			ReactiveParameters mergeParameters = ReactiveParameters.of(mergerFactory, namedParams, unnamedParams,namedState);
			return new Operand(mergerFactory.execute(mergeParameters), ValueType.MAPPER);
		case REACTIVE_TRANSFORMER:
			ReactiveTransformerFactory transformerFactory = Reactive.finderInstance().getTransformerFactory(name);
			List<ReactiveParseProblem> problems = new ArrayList<>();
			ReactiveParameters transParameters = ReactiveParameters.of(transformerFactory, namedParams, unnamedParams,namedState);
			// TODO problems?
			return new Operand(transformerFactory.build(problems, transParameters),ValueType.MAPPER);
		default:
			break;

		}
		// TODO rather throw something
		return null;
	}

	@Override
	public boolean isLiteral() {
		return true;
	}

	@Override
	public Optional<ValueType> returnType() {
		return Optional.of(ValueType.REACTIVE);
	}

	@Override
	public String expression() {
		return expression;
	}
}
