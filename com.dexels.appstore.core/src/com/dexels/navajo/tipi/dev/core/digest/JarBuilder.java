package com.dexels.navajo.tipi.dev.core.digest;

import java.io.File;
import java.io.IOException;

public interface JarBuilder {

	public void buildJar(File source, File destination) throws IOException;

}