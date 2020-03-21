package com.dexels.navajo.functions.util;

import com.dexels.navajo.expression.api.FunctionDefinition;
import com.dexels.navajo.expression.api.nanoimpl.CaseSensitiveXMLElement;
import com.dexels.navajo.expression.api.nanoimpl.XMLElement;
import navajo.ExtensionDefinition;
import navajocore.Version;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Vector;

public class JarFunctionFactory extends FunctionFactoryInterface implements Serializable {

	
	/**
	 * 
	 */
	private static final long serialVersionUID = -5200898919345188706L;
	private static final Logger logger = LoggerFactory
			.getLogger(JarFunctionFactory.class);
	
	
	@Override
	public final void readDefinitionFile(Map<String, FunctionDefinition> fuds, ExtensionDefinition fd) {
		// Read config file.
		CaseSensitiveXMLElement xml = new CaseSensitiveXMLElement();
			
		try {
			InputStream fis = fd.getDefinitionAsStream();
			xml.parseFromStream(fis);
			fis.close();

			Vector<XMLElement> children = xml.getChildren();
			for (int i = 0; i < children.size(); i++) {
				// Get object, usage and description.
				XMLElement element = children.get(i);
				
				if(element.getName().equals("function")) {
					parseFunction(fuds, element);
				}
			}
			
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}
	
	public FunctionDefinition parseFunction(Map<String, FunctionDefinition> fuds,
			XMLElement element) {
		Vector<XMLElement> def = element.getChildren();
		String name = (String) element.getAttribute("name");
		String object = (String) element.getAttribute("class");
		String description = null;
		String inputParams = null;
		String resultParam = null;
		for (int j = 0; j < def.size(); j++) {
			if ( def.get(j).getName().equals("description")) {
				description =  def.get(j).getContent();
			}
			if ( def.get(j).getName().equals("input")) {
				inputParams =  def.get(j).getContent();
			}
			if ( def.get(j).getName().equals("result")) {
				resultParam =  def.get(j).getContent();
			}
		}
		if ( name != null ) {
			FunctionDefinition functionDefinition = new FunctionDefinition(object, description, inputParams, resultParam);
			fuds.put(name, functionDefinition);
			return functionDefinition;
		}
		return null;
	}
	
	@Override
	public void init() {
		Map<String, FunctionDefinition> fuds = getDefaultConfig();
		if(fuds==null) {
			fuds = new HashMap<>();
			setDefaultConfig(fuds);
		}
		ClassLoader myClassLoader = getClass().getClassLoader();

		try {
			Iterator<?> iter = java.util.ServiceLoader.load(Class.forName("navajo.ExtensionDefinition", true, myClassLoader), myClassLoader).iterator();
			while(iter.hasNext()) {
				ExtensionDefinition ed = (ExtensionDefinition) iter.next();
				readDefinitionFile(fuds, ed);
			}
		} catch (Throwable e) {
			logger.debug("ServiceLookup failed. Normal in OSGi environment",e);
			if(!Version.osgiActive()) {
				logger.error("But OSGi isn't active, so something is definitely wrong.",e);
			}
		}
	}

}
