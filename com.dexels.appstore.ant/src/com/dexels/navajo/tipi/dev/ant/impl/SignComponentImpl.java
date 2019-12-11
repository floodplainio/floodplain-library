package com.dexels.navajo.tipi.dev.ant.impl;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.util.HashMap;
import java.util.Map;

import org.osgi.service.component.annotations.Activate;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.ConfigurationPolicy;
import org.osgi.service.component.annotations.Deactivate;
import org.osgi.service.component.annotations.Reference;
import org.osgi.service.component.annotations.ReferencePolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dexels.keystoreprovider.api.KeyStoreProvider;
import com.dexels.navajo.tipi.dev.ant.AntRun;
import com.dexels.navajo.tipi.dev.ant.LoggingOutputStream;
import com.dexels.navajo.tipi.dev.core.projectbuilder.Dependency;
import com.dexels.navajo.tipi.dev.core.sign.SignComponent;

@Component(name="tipi.dev.sign",immediate=true,configurationPolicy=ConfigurationPolicy.REQUIRE)
public class SignComponentImpl implements SignComponent {
	
	
	private final static Logger logger = LoggerFactory
			.getLogger(SignComponentImpl.class);
	private KeyStoreProvider keyStoreProvider;
	private String alias;
	private String storepass;
	private String keypass;
	private String tsaurl;
	
	public SignComponentImpl() {
		logger.info("Signing component created!");
	}
	
	@Activate
	public void activate(Map<String,Object> settings) {
		logger.info("Signing component activated!");		
		this.alias = (String) settings.get("alias");
		this.storepass = (String) settings.get("storepass");
		this.keypass = (String) settings.get("keypass");
		if(this.keypass==null) {
			this.keypass = "";
		}
		if(this.alias==null || this.storepass==null) {
			IllegalArgumentException illegalArgumentException = new IllegalArgumentException("No alias and storepass provided!");
			logger.error("No alias and storepass provided!",illegalArgumentException);
			throw illegalArgumentException;
		}
		this.tsaurl = (String) settings.get("tsaurl");
		if (tsaurl == null) {
		    this.tsaurl="https://timestamp.geotrust.com/tsa";
		}
		
	}
	
	@Deactivate
	public void deactivate() {
		this.alias = null;
		this.storepass = null;
		this.tsaurl = null;
	}
	
	@Reference(policy=ReferencePolicy.DYNAMIC,unbind="clearKeyStoreProvider")
	public void setKeyStoreProvider(KeyStoreProvider keyStoreProvider) {
		this.keyStoreProvider = keyStoreProvider;
	}
	
	public void clearKeyStoreProvider(KeyStoreProvider keyStoreProvider) {
		this.keyStoreProvider = null;
	}

	/* (non-Javadoc)
	 * @see com.dexels.navajo.tipi.dev.ant.impl.SignComponent#signJnlp(java.io.File, java.lang.String, java.lang.String, java.lang.String, java.io.File, java.io.File)
	 */
	@Override
	public void signJnlp(File jnlpFile,String profile, File baseDir) {
		Map<String,String> props = new HashMap<String, String>();
		logger.info("signing with alias: "+alias);
		try {
			
			Map<String,Class<?>> tasks = new HashMap<String,Class<?>>();
			tasks.put("signjar", org.apache.tools.ant.taskdefs.SignJar.class);
			props.put("storepass",storepass);
			props.put("keypass",keypass);
			props.put("alias", alias);
			props.put("tsaurl", tsaurl);
			props.put("profile", profile);
			props.put("keystore",keyStoreProvider.getKeyStorePath().getAbsolutePath());
			props.put("jnlpFile", jnlpFile.getAbsolutePath());
			Logger antlogger = LoggerFactory.getLogger("tipi.appstore.ant");
			PrintStream los = new PrintStream( new LoggingOutputStream(antlogger));
			AntRun.callAnt(getClass().getClassLoader().getResourceAsStream("ant/signjnlp.xml"), baseDir, props,tasks,null,los);
		} catch (IOException e) {
			logger.error("Error: ", e);
		}
	}
	
	   @Override
	   public void signDirectoryCached(File directory,String profile, File baseDir) {
	        Map<String,String> props = new HashMap<String, String>();
	        logger.info("signing with alias: "+alias);
	        try {
	            
	            Map<String,Class<?>> tasks = new HashMap<String,Class<?>>();
	            tasks.put("signjar", org.apache.tools.ant.taskdefs.SignJar.class);
	            props.put("storepass",storepass);
	            props.put("keypass",keypass);
	            props.put("alias", alias);
	            props.put("tsaurl", tsaurl);
	            props.put("profile", profile);
	            props.put("keystore",keyStoreProvider.getKeyStorePath().getAbsolutePath());
	            props.put("dir", directory.getAbsolutePath());
	            props.put("targetname", directory.getName());
	            Logger antlogger = LoggerFactory.getLogger("tipi.appstore.ant");
	            PrintStream los = new PrintStream( new LoggingOutputStream(antlogger));
	            AntRun.callAnt(getClass().getClassLoader().getResourceAsStream("ant/signresources.xml"), baseDir, props,tasks,null,los);
	        } catch (IOException e) {
	            logger.error("Error: ", e);
	        }
	    }

	   @Override
	   public void jarDirectoryWithoutSign(File directory,String profile, File baseDir) {
	        Map<String,String> props = new HashMap<String, String>();
	        try {
	            
	            Map<String,Class<?>> tasks = new HashMap<String,Class<?>>();
	            tasks.put("signjar", org.apache.tools.ant.taskdefs.SignJar.class);
	            props.put("profile", profile);
	            props.put("dir", directory.getAbsolutePath());
	            props.put("targetname", directory.getName());
	            Logger antlogger = LoggerFactory.getLogger("tipi.appstore.ant");
	            PrintStream los = new PrintStream( new LoggingOutputStream(antlogger));
	            AntRun.callAnt(getClass().getClassLoader().getResourceAsStream("ant/unsignedresources.xml"), baseDir, props,tasks,null,los);
	        } catch (IOException e) {
	            logger.error("Error: ", e);
	        }
	    }

	/* (non-Javadoc)
	 * @see com.dexels.navajo.tipi.dev.ant.impl.SignComponent#signdependency(com.dexels.navajo.tipi.dev.core.projectbuilder.Dependency, java.lang.String, java.lang.String, java.io.File, java.io.File)
	 */
	@Override
	public void signdependency(Dependency d, String securityHeader, File repo) {
		Map<String,String> props = new HashMap<String, String>();
		try {
			Map<String,Class<?>> tasks = new HashMap<String,Class<?>>();
			tasks.put("signjar", org.apache.tools.ant.taskdefs.SignJar.class);
			props.put("storepass",storepass);
			props.put("keypass",keypass);
			props.put("alias", alias);
			props.put("tsaurl", tsaurl);
			props.put("keystore",keyStoreProvider.getKeyStorePath().getAbsolutePath());
			File cachedFile = d.getFilePathForDependency(repo, securityHeader);
			props.put("jarfile", cachedFile.getAbsolutePath());
			Logger antlogger = LoggerFactory.getLogger("tipi.appstore.ant");
			PrintStream los = new PrintStream( new LoggingOutputStream(antlogger));
			logger.debug("Signing: "+d.getFilePathForDependency(repo, securityHeader));
			AntRun.callAnt(getClass().getClassLoader().getResourceAsStream("ant/signsingle.xml"), repo, props,tasks,null,los);
			logger.info("Signing {} complete ",cachedFile.getAbsolutePath());
		} catch (IOException e) {
			logger.error("Error: ", e);
		} catch (Throwable t) {
			logger.error("Error: ", t);
		}
	}
}
