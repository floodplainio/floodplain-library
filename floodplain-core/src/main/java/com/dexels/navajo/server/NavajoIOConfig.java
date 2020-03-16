package com.dexels.navajo.server;

import com.dexels.navajo.document.Navajo;

import java.io.*;

public interface NavajoIOConfig {
	
	public File getContextRoot();

	@Deprecated
	public InputStream getScript(String name) throws IOException;
	
	@Deprecated
	public InputStream getScript(String name, String tenant,String extension) throws IOException;
	
	public InputStream getConfig(String name) throws IOException;
    public InputStream getResourceBundle(String name) throws IOException;
    public Writer getOutputWriter(String outputPath, String scriptPackage, String scriptName, String extension) throws IOException;
    public Reader getOutputReader(String outputPath, String scriptPackage, String scriptName, String extension) throws IOException;
    public String getConfigPath();
	public String getRootPath();
	public String getScriptPath();
	public String getCompiledScriptPath();
	public String getAdapterPath();
	public String getResourcePath();
	public String getDeployment();

	public Navajo readConfig(String s) throws IOException;
	/**
	 * Name does not include tenant suffix
	 */
	public boolean hasTenantScriptFile(String rpcName, String tenant, String scriptPath);
    

}
