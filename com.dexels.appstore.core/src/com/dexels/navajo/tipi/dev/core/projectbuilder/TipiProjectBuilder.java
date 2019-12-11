package com.dexels.navajo.tipi.dev.core.projectbuilder;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.Charset;
import java.util.PropertyResourceBundle;
import java.util.StringTokenizer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dexels.navajo.tipi.dev.core.util.XMLElement;

public abstract class TipiProjectBuilder {
	
	private final static Logger logger = LoggerFactory
			.getLogger(TipiProjectBuilder.class);
	
	private boolean useVersioning;
	public abstract void downloadExtensionJars(String extensionName, String version, URL remoteExtensionUrl, XMLElement extensionElement, File baseDir, boolean clean, boolean localSign) throws MalformedURLException, IOException;

	
	public void setUseVersioning(boolean b) {
		useVersioning = b;
	}
	
	protected boolean useJnlpVersioning() {
		return useVersioning;
	}
	
	public void downloadProjectInclude(URL remoteExtensionUrl,  File baseDir, boolean clean) throws MalformedURLException, IOException {
		URL projectInclude = new URL(remoteExtensionUrl,"projectinclude.zip");
	   try {
			ClientActions.downloadFile(projectInclude, "projectinclude.zip", baseDir, clean, false);
			File projectZip = new File(baseDir,"projectinclude.zip");
			ClientActions.unzip(projectZip, baseDir);
	   } catch (Exception e) {
			logger.info("No project include found!");
		}

	}

    private PropertyResourceBundle readBundle(File f) throws FileNotFoundException, IOException {
    	if(f==null || !f.exists()) {
    		return null;
    	}
    	try(FileInputStream fis = new FileInputStream(f)) {
    		InputStreamReader isr = new InputStreamReader(fis,Charset.forName("UTF-8"));
    		return new PropertyResourceBundle(isr);
    	}
    }
	// Will return a version label for a certain extension, based on the previous build, or null when things are amiss.
	protected String getCurrentExtensionVersion(File baseDir, String extensionName) throws IOException {
		File previousBuild = new File(baseDir,"settings/buildresults.properties");
		if(!previousBuild.exists()) {
			return null;
		}
		PropertyResourceBundle pe = readBundle(previousBuild);
		String extensions = pe.getString("extensions");
		StringTokenizer st = new StringTokenizer(extensions,",");
		while(st.hasMoreTokens()) {
			String current = st.nextToken();
			if(current.indexOf("/")==-1 ) {
				logger.info("No version in version result: "+extensions+" currently checking: "+current);
				return null;
			}
			StringTokenizer st2 = new StringTokenizer(current,"/");
			String name = st2.nextToken();
			if(!name.equals(extensionName)) {
				continue;
			}
			String version = st2.nextToken();
			return version;
		}
		return null;
	}

}
