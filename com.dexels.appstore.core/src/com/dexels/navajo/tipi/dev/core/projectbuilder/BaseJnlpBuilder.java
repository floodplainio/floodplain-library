package com.dexels.navajo.tipi.dev.core.projectbuilder;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.ResourceBundle;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dexels.navajo.tipi.dev.core.util.CaseSensitiveXMLElement;
import com.dexels.navajo.tipi.dev.core.util.XMLElement;

public abstract class BaseJnlpBuilder extends BaseDeploymentBuilder {

	
	private final static Logger logger = LoggerFactory
			.getLogger(BaseJnlpBuilder.class);
	

	protected boolean appendResourceForExtension(XMLElement resources,
			String name, String version, boolean main, String masterrevision) throws IOException {
		XMLElement x = new CaseSensitiveXMLElement("jar");
		resources.addChild(x);
		if(version!=null) {
			x.setAttribute("version", version);
		}
		if (masterrevision!=null) {
			x.setAttribute("href", "lib/_"+masterrevision+"/" + name);
		} else {
			x.setAttribute("href", "lib/" + name);
		}
		if(main) {
			x.setAttribute("main", "true");
		}
		return false;
	}


	protected abstract void appendProxyResource(XMLElement resources, String repository, String mainExtension, boolean useVersioning) throws IOException;
	public abstract String getJnlpName();


	public String build(String repository, String developRepository, String extensions, Map<String,String> tipiProperties, String deployment,  File baseDir, String codebase, List<String> profiles, boolean useVersioning) {
		for (String fileName : profiles) {
			File jnlpFile = new File(baseDir, fileName+".jnlp");
			logger.debug("Writing jnlp: "+jnlpFile.getAbsolutePath());
			try {
				FileWriter fw1 = new FileWriter(jnlpFile);
				fw1.write("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n");
//				XMLElement output = buildElement(repository, extensions,tipiProperties, deployment,baseDir, codebase,"", fileName+".jnlp", fileName,useVersioning);
//				output.write(fw1);
				fw1.flush();
				fw1.close();
			} catch (IOException e) {
				logger.error("Error: ",e);
			}
		}
		return null;
} 
	

	private void appendArguments(XMLElement app, XMLElement java, Map<String, String> elements) {
		String args = elements.get("arguments");
		
		for (String elt : elements.keySet()) {
			if (elt.equals("javaVm")) {
				java.setAttribute("java-vm-args", elements.get(elt));
			} else if (elt.equals("javaVersion")) {
				java.setAttribute("version", elements.get(elt));
			} else if(elt.equals("main-class")) {
				app.setAttribute("main-class", elements.get(elt));

			} else {
				//  if an args setting is supplied, no arguments will be appended, as they will be explicitly listed in the args
				if(args==null) {
					XMLElement zz = new CaseSensitiveXMLElement("argument");
					zz.setContent(elt + "=" + elements.get(elt));
					app.addChild(zz);
				}
			}
		}
		if(elements.get("main-class")==null) {
			app.setAttribute("main-class", "tipi.MainApplication");
		}
		if(args!=null) {
			String[] parts = args.split(",");
			for (String element : parts) {
				XMLElement zz = new CaseSensitiveXMLElement("argument");
				zz.setContent(element.trim());
				app.addChild(zz);
			}
		}
	}

	private void appendSecurity(XMLElement output, String security) {

		if ("all".equals(security)) {
			XMLElement sec = output.addTagKeyValue("security", "");
			sec.addTagKeyValue("all-permissions", "");
		} else if ("j2ee".equals(security)) {
			XMLElement sec = output.addTagKeyValue("security", "");
			sec.addTagKeyValue("j2ee-application-client-permissions", "");
		}else if ("none".equals(security)) {
            XMLElement sec = output.addTagKeyValue("security", "");
            sec.addTagKeyValue("sandbox", "");
        }
		

	}
	
	@Override
	public void buildFromMaven(ResourceBundle settings,
			List<Dependency> dependencyList, File appFolder,
			List<String> profiles, String resourceBase, String suppliedCodebase, String applicationName,String deployment,  Map<String,Object> repoSettings) {
		logger.debug("Building in folder: "+appFolder);
		if(profiles==null) {
			logger.warn("No profiles loaded, not building jnlp");
			return;
		}
		for (String fileName : profiles) {
			File jnlpFile = new File(appFolder, fileName+".jnlp");
			logger.info("Writing jnlp: "+jnlpFile.getAbsolutePath()+" with # of dependencies: "+dependencyList.size());
			try {
				FileWriter fw1 = new FileWriter(jnlpFile);
				fw1.write("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n");
				XMLElement output = buildElementFromMaven(dependencyList,settings,deployment,appFolder, resourceBase,fileName+".jnlp", fileName,suppliedCodebase,applicationName,repoSettings);
				output.write(fw1);
				fw1.flush();
				fw1.close();
			} catch (IOException e) {
				logger.error("Error: ",e);
			}
		}
	}


	private XMLElement buildElementFromMaven(List<Dependency> dependencies, ResourceBundle params,String deployment, File baseDir, String resourceBase, String fileName, String profile, String suppliedCodebase, String applicationName, Map<String,Object> repoSettings) throws IOException {

		Map<String, String> arguments;
		try {
			arguments = parseArguments(baseDir, profile,deployment);
		} catch (IOException e1) {
			logger.error("Error: ", e1);
			arguments = new LinkedHashMap<String, String>();
		}

		
		XMLElement output = new CaseSensitiveXMLElement();
		output.setName("jnlp");
		output.setAttribute("version", "1");
		output.setAttribute("spec", "1.0+");
		output.setAttribute("codebase", assembleCodebase(suppliedCodebase,applicationName,"apps"));
		output.setAttribute("href", profile+".jnlp");

		XMLElement information = output.addTagKeyValue("information", "");
		information.addTagKeyValue("title", findAttribute("title",params,arguments));
		information.addTagKeyValue("vendor",  findAttribute("vendor",params,arguments));
		information.addTagKeyValue("homepage",  findAttribute("homepage",params,arguments));
		if (resourceBase!=null && !"".equals(resourceBase)) {
			information.addTagKeyValue("icon", "").setAttribute("href", resourceBase+"/"+  findAttribute("icon",params,arguments));
		} else {
			information.addTagKeyValue("icon", "").setAttribute("href",  findAttribute("icon",params,arguments));
		}
		

		logger.debug("Parsing: " + fileName);

		if ( findAttribute("splash",params,arguments) != null) {
			XMLElement splash = new CaseSensitiveXMLElement();
			splash.setName("icon");
			information.addChild(splash);
			if (resourceBase!=null && !"".equals(resourceBase)) {
				splash.setAttribute("href", resourceBase+"/" + findAttribute("splash",params,arguments));
			} else {
				splash.setAttribute("href",findAttribute("splash",params,arguments));
			}
			splash.setAttribute("kind", "splash");
		}

		XMLElement resources = output.addTagKeyValue("resources", "");
		
        appendProperties(resources, arguments);
 
		XMLElement java = resources.addTagKeyValue("j2se", "");
		String jvmVersion = (String) repoSettings.get("java.version");
		if(jvmVersion==null) {
			jvmVersion = "1.7+";
		}
		java.setAttribute("version", jvmVersion);
		appendSecurity(output, params.getString("permissions"));
		String masterrevision = (String) repoSettings.get("masterrevision");
		for (Dependency dependency : dependencies) {
			appendResourceForExtension(resources, dependency.getFileName(),dependency.getVersion(),false,masterrevision);
		}

		appendResourceForExtension(resources, profile+"_jnlp.jar",null,true,null);
		
		File tipiDigest = new File(baseDir,"tipi/remotedigest.properties");
        if(tipiDigest.exists()) {
            appendResourceForExtension(resources, "tipi.jar",null,false,null);
        } else {
            logger.warn("Not including tipi.jar due to missing remotedigest.properties!");
        }
        File resourceDigest = new File(baseDir,"resource/remotedigest.properties");
        if(resourceDigest.exists()) {
            appendResourceForExtension(resources, "resource.jar",null,false,null);
        } else {
            logger.warn("Not including resource.jar.jar due to missing remotedigest.properties!");
        }
		
		XMLElement app = output.addTagKeyValue("application-desc", "");


		if (resourceBase!=null && !"".equals(resourceBase)) {
			arguments.put("resourceCodeBase", resourceBase+"/resource/");
			arguments.put("tipiCodeBase", resourceBase+"/tipi/");
		}
		arguments.put("tipi.appstore.application", applicationName);
		arguments.put("tipi.appstore.websocketurl", createWebsocketUrl(suppliedCodebase, applicationName));
		arguments.put("tipi.appstore.tenant", profile);
		arguments.put("tipi.appstore.session", "notused");
		arguments.put("tipi.appstore.deployment", deployment);
		
//		appendArguments(app, java, params);
		XMLElement tipiStart = null;
		File startXml = new File(baseDir,"tipi/start.xml");
		if(startXml.exists()) {
			tipiStart = new CaseSensitiveXMLElement("argument");
			tipiStart.setContent("start.xml");
		}
	
		appendArguments(app, java, arguments);
		
		if(tipiStart!=null) {
			app.addChild(tipiStart);
		}
		return output;
	}


    private void appendProperties(XMLElement resources, Map<String, String> arguments) {
        XMLElement prop = resources.addTagKeyValue("property", "");
        prop.setAttribute("name", "jnlp.versionEnabled");
        prop.setAttribute("value", "true");
        
        for (String elt : arguments.keySet()) {
            if (elt.equals("sysProps")) {
                String sysPropsString =  arguments.get(elt);
                String[] pairs = sysPropsString.split(" ");
                for (String pair : pairs) {
                    String[] keyvalue = pair.split("=");
                    if (keyvalue.length != 2) {
                        logger.warn("Invalid property: {}", pair);
                        continue;
                    }
                    XMLElement proparg = resources.addTagKeyValue("property", "");
                    proparg.setAttribute("name", keyvalue[0]);
                    proparg.setAttribute("value", keyvalue[1]);
                }
               
            }
        }
    }

	private String findAttribute(String element, ResourceBundle params, Map<String, String> arguments) {
		if(params.containsKey(element)) {
			return params.getString(element);
		}
		return arguments.get(element);
	}


	private String createWebsocketUrl(String codebase, String applicationName) {
		String url = assembleCodebase(codebase, null,null);
		String websock = url.replaceAll("http", "ws");
		return websock+"websocket";
	}

	private String assembleCodebase(String codebase, String applicationName,String context) {
		if(codebase==null) {
			return "$$codebase";
		}
		StringBuilder sb = new StringBuilder();
		sb.append(codebase);
		if(!codebase.endsWith("/")) {
			sb.append("/");
		}
		if(context!=null && !"".equals(context)) {
			sb.append(context);
			sb.append("/");
		}
		if(applicationName!=null && !"".equals(applicationName)) {
			sb.append(applicationName);
		}
		return sb.toString();
	}

	
}
