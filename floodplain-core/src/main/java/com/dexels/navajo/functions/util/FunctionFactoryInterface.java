package com.dexels.navajo.functions.util;

import java.io.Serializable;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.dexels.navajo.expression.api.FunctionInterface;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dexels.navajo.expression.api.FunctionDefinition;
import com.dexels.navajo.expression.api.StatefulFunctionInterface;
import com.dexels.navajo.expression.api.TMLExpressionException;
import com.dexels.navajo.script.api.UserException;

import navajo.ExtensionDefinition;

public abstract class FunctionFactoryInterface implements Serializable {

	private static final long serialVersionUID = 6512562097288200226L;

	private transient Map<String, FunctionDefinition> defaultConfig = null;

	protected final transient Map<ExtensionDefinition,Map<String, FunctionDefinition>> adapterConfig = new HashMap<>();
	
	protected final transient Map<ExtensionDefinition,Map<String,FunctionDefinition>> functionConfig = new HashMap<>();

	private static Object semaphore = new Object();
	private boolean initializing = false;
	private final transient List<FunctionResolver> functionResolvers = new LinkedList<>();
	private static final Logger logger = LoggerFactory.getLogger(FunctionFactoryInterface.class);
	public abstract void init();
	
	public void injectExtension(ExtensionDefinition fd) {
		readDefinitionFile(getConfig(fd), fd);

	}
	
	public void addExplicitFunctionDefinition(String name, FunctionDefinition fd) {
		if(defaultConfig==null) {
			defaultConfig = new HashMap<>();
		}
		defaultConfig.put(name, fd);
	}

	
	public abstract void readDefinitionFile(Map<String, FunctionDefinition> fuds, ExtensionDefinition fd) ;

	private final FunctionDefinition getDef(String name)  {
		if(defaultConfig!=null) {
			FunctionDefinition fd = defaultConfig.get(name);
			if(fd!=null) {
				return fd;
			}
		} else {
			logger.debug("No default config");
		}
		for (FunctionResolver fr : functionResolvers) {
			FunctionDefinition fd = fr.getFunction(name);
			if(fd!=null) {
				return fd;
			}
		}
		
		for (Map<String, FunctionDefinition> elt : functionConfig.values()) {
			FunctionDefinition fd = elt.get(name);
			if(fd!=null) {
				return fd;
			}
		}
		return null;
	}
	/**
	 * Fetch a functiondefinition. If not found first time, try re-init (maybe new definition), if still not found throw Exception.
	 * 
	 * @param name
	 * @return
	 * @throws UserException
	 */
	
	public final FunctionDefinition getDef(ExtensionDefinition ed, String name) {
		
		while ( initializing ) {
			// Wait a bit.
			synchronized (semaphore) {
				try {
					semaphore.wait(1000);
				} catch (InterruptedException e) {
					logger.error("Caught exception. ",e);
				}
			}
		}
		
		Map<String, FunctionDefinition> map = functionConfig.get(ed);
		if(map==null) {
			logger.warn("Function definition not found: {} for extensiondef: {} map: {}",name,ed.getId(),functionConfig);
			throw new TMLExpressionException("Could not find function definition: " + name);
		} else {
			FunctionDefinition fd = map.get(name);
			if ( fd != null ) {
				return fd;
			} else {
				throw new TMLExpressionException("Could not find function definition: " + name);
			}
		}
	}
	
	public  String getAdapterClass(String name, ExtensionDefinition ed)  {
		FunctionDefinition functionDefinition = getAdapterConfig(ed).get(name);
		if(functionDefinition==null) {
			logger.info("No function definition found for: {}, assuming class name.",name);
			return name;
		}
		return functionDefinition.getObject().trim();
	}

	public Set<String> getFunctionNames(ExtensionDefinition ed) {
		final Map<String, FunctionDefinition> functionsForExtension = functionConfig.get(ed);
		if(functionsForExtension==null) {
			logger.error("Error listing function names for definition: {} id: {}",ed.getDescription(),ed.getId());
			return null;
		}
		return functionsForExtension.keySet();
	}
	public void clearFunctionNames() {
		functionConfig.clear();
	}

	public Set<String> getAdapterNames(ExtensionDefinition ed) {
		return getAdapterConfig(ed).keySet();
	}

	@SuppressWarnings("unchecked")
	public FunctionInterface getInstance(final ClassLoader cl, final String functionName)  {
		try {
			FunctionDefinition fd = getDef(functionName);
			if(fd==null) {
				logger.error("Missing function definition: {}",functionName);
				return null;
			}
			Class<FunctionInterface> myClass = (Class<FunctionInterface>) Class.forName(fd.getObject(), true, cl);
			FunctionInterface fi =myClass.getDeclaredConstructor(). newInstance();
			fi.setDefinition(fd);
			if (!fi.isInitialized()) {
				fi.setTypes(fd.getInputParams(), fd.getResultParam());
			}
			return fi;
		} catch (Exception e) {
			logger.error("Function: "+functionName+" not found!",e);
			return null;
		}
	}

	
	
	private Map<String, FunctionDefinition> getConfig(ExtensionDefinition ed) {
		Map<String, FunctionDefinition> map = functionConfig.get(ed);
		if(map!=null) {
			return map;
		}
		map = new HashMap<>();
		functionConfig.put(ed,map);
		return map;
	}
	
	public Map<String, FunctionDefinition> getAdapterConfig(ExtensionDefinition ed) {
		Map<String, FunctionDefinition> map = adapterConfig.get(ed);
		if(map!=null) {
			return map;
		}
		map = new HashMap<>();
		adapterConfig.put(ed,map);
		return map;
	}
	

	public void setAdapterConfig(ExtensionDefinition ed, Map<String, FunctionDefinition> config) {
		this.adapterConfig.put(ed, config);
	}

	public void setConfig(ExtensionDefinition ed, Map<String, FunctionDefinition> config) {
		this.functionConfig.put(ed, config);
	}

	public Map<String, FunctionDefinition> getDefaultConfig() {
		return defaultConfig;
	}

	public void setDefaultConfig(Map<String, FunctionDefinition> config) {
		this.defaultConfig = config;
	}


}
