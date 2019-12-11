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
import com.dexels.navajo.tipi.dev.core.digest.DigestBuilder;

@Component(name="tipi.dev.getdown.digest")
public class GetDownDigest implements DigestBuilder {
	/* (non-Javadoc)
	 * @see com.dexels.navajo.tipi.dev.ant.impl.DigestBuilder#createDigests(java.io.File)
	 */
	@Override
	public void buildGetDown(File destination) throws IOException {
        Map<String,Class<?>> tasks = new HashMap<String,Class<?>>();
        tasks.put("digest", com.threerings.getdown.tools.DigesterTask.class);
		Map<String,String> props = new HashMap<String, String>();
		props.put("dir", destination.getAbsolutePath());
        Logger antlogger = LoggerFactory.getLogger("tipi.appstore.ant");
		PrintStream los = new PrintStream( new LoggingOutputStream(antlogger));
		System.err.println(">>> Using folder: "+destination);
		AntRun.callAnt(getClass().getClassLoader().getResourceAsStream("ant/getdown.xml"), destination, props, tasks, null, los);
	}
}
