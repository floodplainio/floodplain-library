package com.dexels.navajo.tipi.dev.core.sign;

import java.io.File;


import com.dexels.navajo.tipi.dev.core.projectbuilder.Dependency;

public interface SignComponent {

	public void signJnlp(File jnlpFile, String profile, File baseDir);
	public void signDirectoryCached(File dir, String profile, File baseDir);


	public void signdependency(Dependency d, String securityHeader, File repo);
	public void jarDirectoryWithoutSign(File directory, String profile, File baseDir);

}