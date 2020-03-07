package com.dexels.navajo.server;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FilenameFilter;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dexels.navajo.document.Header;
import com.dexels.navajo.document.Navajo;
import com.dexels.navajo.document.NavajoException;
import com.dexels.navajo.document.NavajoFactory;
import com.dexels.navajo.loader.NavajoClassLoader;
import com.dexels.navajo.mapping.CompiledScript;
import com.dexels.navajo.mapping.MappingUtils;
import com.dexels.navajo.script.api.Access;
import com.dexels.navajo.script.api.AuthorizationException;
import com.dexels.navajo.script.api.CompiledScriptInterface;
import com.dexels.navajo.script.api.Dependency;
import com.dexels.navajo.script.api.NavajoClassSupplier;
import com.dexels.navajo.script.api.SystemException;
import com.dexels.navajo.script.api.UserException;
import com.dexels.navajo.util.AuditLog;

import navajocore.Version;

/**
 * Title:        Navajo
 * Description:
 * Copyright:    Copyright (c) 2001 - 2011
 * Company:      Dexels
 * @author Arjen Schoneveld en Martin Bergman
 * @version $Id$
 *
 * This class is used to run Navajo script web services.
 *
 * DISCLAIMER
 *
 * THIS SOFTWARE IS PROVIDED ``AS IS'' AND ANY EXPRESSED OR IMPLIED
 * WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES
 * OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED.  IN NO EVENT SHALL DEXELS BV OR ITS CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY,
 * OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,
 * PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA,
 * OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR
 * TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT
 * OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
 * SUCH DAMAGE.
 * ====================================================================
 */

public class GenericHandler extends ServiceHandler {

    private static final Map<String,NavajoClassSupplier> loadedClasses = new ConcurrentHashMap<>();

    private static Object mutex1 = new Object();
   
    
	private static final Logger logger = LoggerFactory
			.getLogger(GenericHandler.class);
    
    private NavajoConfigInterface tenantConfig;

	protected NavajoConfigInterface navajoConfig;

    public GenericHandler() {

    }
    
	@Override
	public String getIdentifier() {
		return "default";
	}

	@Override
	public void setNavajoConfig(NavajoConfigInterface navajoConfig) {
    	this.tenantConfig = navajoConfig;

	}

	public static void doClearCache() {
    	// fix ugly shutdown npe in some cases
    	if(loadedClasses!=null) {
        	loadedClasses.clear();
    	}
    }

    private static final CompiledScript getCompiledScript(Access a, String className,File scriptFile,String scriptName) throws Exception {
    	NavajoClassSupplier loader = getScriptLoader(a.betaUser, className);
    	Class<?> cs = loader.getCompiledNavaScript(className);
    	if ( cs != null ) {
    		com.dexels.navajo.mapping.CompiledScript cso = (com.dexels.navajo.mapping.CompiledScript) cs.getDeclaredConstructor().newInstance();
    		cso.setClassLoader(loader);
    		return cso;
    	}
    	return null;
    }
    
    private static final boolean hasDirtyDepedencies(Access a, String className) {
    	CompiledScript cso = null;
    	try {
    		NavajoClassLoader loader = (NavajoClassLoader) loadedClasses.get(className);
    		if ( loader == null ) { // Script does not yet exist.
    			return false;
    		}
    		Class<?> cs = loader.getCompiledNavaScript(className);
        	if ( cs != null ) {
        		cso = (CompiledScript) cs.getDeclaredConstructor().newInstance();
        		cso.setClassLoader(loader);
        	}	
    	} catch (Exception e) {
    		logger.error("Error: ", e);
    		return false;
    	}
    	if ( cso != null ) {
    		return cso.hasDirtyDependencies(a);
    	} else {
    		return false;
    	}

    }
    
    private final Object[] getScriptPathServiceNameAndScriptFile(Access a, String rpcName, boolean betaUser) throws Exception {
    	String scriptPath = DispatcherFactory.getInstance().getNavajoConfig().getScriptPath();
    	int strip = rpcName.lastIndexOf("/");
        String pathPrefix = "";
        String serviceName = rpcName;
        if (strip != -1) {
          serviceName = rpcName.substring(strip+1);
          pathPrefix = rpcName.substring(0, strip) + "/";
        }
        final String applicationGroup;
        if (a.getTenant()==null) {
        	applicationGroup = this.tenantConfig.getInstanceGroup();
		} else {
			applicationGroup = a.getTenant();
		}
        
    	File scriptFile = new File(scriptPath + "/" + rpcName + "_" + applicationGroup + ".xml");
    	if (scriptFile.exists()) {
    		serviceName += "_" + applicationGroup;
    	} else {
    		scriptFile = new File(scriptPath + "/" + rpcName + ( betaUser ? "_beta" : "" ) + ".xml" );
    		if ( betaUser && !scriptFile.exists() ) {
    			// Try normal webservice.
    			scriptFile = new File(scriptPath + "/" + rpcName + ".xml" );
    		} else if ( betaUser ) {
    			serviceName += "_beta";
    		} 
    		// Check if scriptFile exists.
    	}
    	
    	if(!scriptFile.exists()) {
       	//-------------------------------
       	// Now, check the context folder:
       	//-------------------------------
       	String sp = "scripts/";
       	File scrPath = new File(DispatcherFactory.getInstance().getNavajoConfig().getContextRoot(),sp);
       	scriptFile = new File(scrPath + rpcName + "_" + applicationGroup + ".xml");
       	if (scriptFile.exists()) {
       		serviceName += "_" + applicationGroup;
       	} else {
       		scriptFile = new File(scrPath, rpcName + ( betaUser ? "_beta" : "" ) + ".xml" );
       		if ( betaUser && !scriptFile.exists() ) {
       			// Try normal webservice.
       			scriptFile = new File(scrPath, rpcName + ".xml" );
       		} else if ( betaUser ) {
       			serviceName += "_beta";
       		} 
       		// Check if scriptFile exists.
   			if ( !scriptFile.exists() ) {
   			}
       	}

    	}
    	
    	
    	
    	
    	String sourceFileName = null;
    	if(scriptFile.exists()) {
    		sourceFileName = DispatcherFactory.getInstance().getNavajoConfig().getCompiledScriptPath()+ "/" + pathPrefix + serviceName + ".java";
    		// regular scriptfile, 
    	} else {
    		// pure java scriptfile
    		String temp =DispatcherFactory.getInstance().getNavajoConfig().getScriptPath()+ "/" + pathPrefix + serviceName + ".java";
    		File javaScriptfile = new File(temp);
    		if(javaScriptfile.exists()) {
       		sourceFileName = temp;    			    			
			}
		}
    	// set when either normal script or pure java, will skip if its neither (-> a jsr223 script)
		if (sourceFileName != null) {
			String classFileName = DispatcherFactory.getInstance().getNavajoConfig().getCompiledScriptPath() + "/" + pathPrefix
					+ serviceName + ".class";
			String className = (pathPrefix.equals("") ? serviceName : MappingUtils.createPackageName(pathPrefix) + "."
					+ serviceName);
			return new Object[] { pathPrefix, serviceName, scriptFile, sourceFileName, new File(sourceFileName), className,
					classFileName, new File(classFileName),true };
		}
    	
    	
		File currentScriptDir = new File(DispatcherFactory.getInstance().getNavajoConfig().getScriptPath() + "/" + pathPrefix);
		final String servName = serviceName;
		File[] scripts = currentScriptDir.listFiles(new FilenameFilter() {
			@Override
			public boolean accept(File dir, String name) {
				return name.startsWith(servName);
			}
		});
		if(scripts==null) {
			return null;
		}
		
		if (scripts.length > 1) {
			logger.warn("Warning, multiple candidates. Assuming the first: " + scripts[0].getName());
		}
		if(scripts.length==0) {
			logger.warn("Not found. dir: "+currentScriptDir.getAbsolutePath());
			return null;
		}
		String classFileName = null;
		String className = "com.dexels.navajo.server.scriptengine.GenericScriptEngine";    	
    	
    	return new Object[]{pathPrefix,serviceName,scriptFile,sourceFileName,scripts[0],className,classFileName,null,false};

    }
    
    /**
     * Check whether Navajo script file needs to be recompiled.
     * 
     * @param scriptFile
     * @param sourceFile
     * @return
     */
    private static final boolean checkScriptRecompile(File scriptFile, File sourceFile) {
    	if ( scriptFile == null ) {
    		return false;
    	}
    	return (!sourceFile.exists() || (scriptFile.lastModified() > sourceFile.lastModified()) || sourceFile.length() == 0);
    }

    
    private static final NavajoClassSupplier getScriptLoader(boolean isBetaUser, String className) {
    	NavajoClassSupplier newLoader = loadedClasses.get(className);
         if (newLoader == null ) {
         	newLoader = new NavajoClassLoader(null, DispatcherFactory.getInstance().getNavajoConfig().getCompiledScriptPath(), 
         			DispatcherFactory.getInstance().getNavajoConfig().getClassloader() );
         	// Use concurrent hashmap: if key exists, return existing classloader.
         	NavajoClassSupplier ncs = loadedClasses.putIfAbsent(className, newLoader);
         	if ( ncs != null ) {
         		return ncs;
         	}
         }
         return newLoader;
    }



    /**
     * doService() is called by Dispatcher to perform web service.
     *
     * @return
     * @throws NavajoException
     * @throws UserException
     * @throws SystemException
     * @throws AuthorizationException
     */
	@Override
    public final Navajo doService( Access a ) throws UserException, SystemException, AuthorizationException {
		return null;
    }

}
