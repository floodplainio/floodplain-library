package com.dexels.navajo.tipi.dev.ant.impl;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.util.HashMap;
import java.util.Map;

import org.osgi.service.component.annotations.Component;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dexels.navajo.tipi.dev.ant.AntRun;
import com.dexels.navajo.tipi.dev.ant.LoggingOutputStream;
import com.dexels.navajo.tipi.dev.core.digest.JarBuilder;

@Component(name="tipi.dev.tipijars")
public class CreateTipiJars implements JarBuilder {
	/* (non-Javadoc)
	 * @see com.dexels.navajo.tipi.dev.ant.impl.DigestBuilder#createDigests(java.io.File)
	 */
	@Override
	public void buildJar(File source, File destination) throws IOException {
        Map<String,Class<?>> tasks = new HashMap<String,Class<?>>();
		Map<String,String> props = new HashMap<String, String>();
		props.put("source", source.getAbsolutePath());
		props.put("destination", destination.getAbsolutePath());
        Logger antlogger = LoggerFactory.getLogger("tipi.appstore.ant");
		PrintStream los = new PrintStream( new LoggingOutputStream(antlogger));
		System.err.println(">>> Using folder: "+destination);
		AntRun.callAnt(getClass().getClassLoader().getResourceAsStream("ant/createjars.xml"), destination, props, tasks, null, los);
		System.err.println("done");
	}
}
