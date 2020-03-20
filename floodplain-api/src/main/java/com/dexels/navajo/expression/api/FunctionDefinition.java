package com.dexels.navajo.expression.api;


import com.dexels.immutable.api.ImmutableMessage;
import com.dexels.immutable.api.ImmutableMessage.ValueType;
import com.dexels.immutable.api.ImmutableMessageParser;
import com.dexels.immutable.api.ImmutableTypeParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;

public final class FunctionDefinition implements Serializable {

	
	private static final Logger logger = LoggerFactory
			.getLogger(FunctionDefinition.class);
	private static final long serialVersionUID = 8105107847721249814L;
	// = fully qualified class name, actually
	private final String object;
	private final String description;
	private final String [][] inputParams;
	private final ValueType [] resultParam;
	private Class<? extends FunctionInterface> functionClass;
	
	public FunctionDefinition(final String object, final String description, final String inputParams, final String resultParam) {
		this.object = object;
		this.description = description;
		if ( inputParams != null && !inputParams.equals("") ) {
			String [] params = inputParams.split(",");
			this.inputParams = new String[params.length][];
			for (int i = 0; i < params.length; i++) {
				this.inputParams[i] = params[i].split("\\|");
			}
		} else {
			this.inputParams =  null;
		}
		if ( resultParam != null && !resultParam.equals("") ) {
			String[] res = resultParam.split("\\|");

			this.resultParam = Arrays.asList(res).stream().map(ImmutableTypeParser::parseType).toArray(ValueType[]::new);
		} else {
			this.resultParam = null;
		}
	}

	public final String getObject() {
		return object;
	}

	public final String getDescription() {
		return description;
	}

	public final String [][] getInputParams() {
		return inputParams;
	}

	public final ValueType[] getResultParam() {
		return resultParam;
	}

	@Override
	public String toString() {
		return description;
	}
	
	public static void main(String [] args) throws Exception {
		
	}

	public Class<? extends FunctionInterface> getFunctionClass() {
		return functionClass;
	}

	public void setFunctionClass(Class<? extends FunctionInterface> clz) {
		this.functionClass = clz;
	}
	
	public FunctionInterface getFunctionInstance() {
		try {
			final Class<? extends FunctionInterface> fc = getFunctionClass();
			if(fc==null) {
				logger.warn("No function class found for function with name: {}", getDescription());
				return null;
			}
			FunctionInterface osgiResolution = fc.getDeclaredConstructor().newInstance();
			if (!osgiResolution.isInitialized()) {
				osgiResolution.setTypes(getInputParams(), getResultParam());
			}
			return osgiResolution;
		} catch (InstantiationException|IllegalAccessException|IllegalArgumentException|InvocationTargetException|NoSuchMethodException|SecurityException e) {
			logger.warn("Function instantiation issue.",e);
		return null;
		}
	}
}
