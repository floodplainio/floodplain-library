package com.dexels.navajo.tipi.dev.server.appmanager.operations.impl;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.security.DigestOutputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Map;
import java.util.Properties;
import java.util.zip.GZIPOutputStream;

import javax.servlet.Servlet;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.apache.commons.io.IOUtils;
import org.osgi.service.component.annotations.Activate;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.Deactivate;
import org.osgi.service.component.annotations.Reference;
import org.osgi.service.component.annotations.ReferenceCardinality;
import org.osgi.service.component.annotations.ReferencePolicy;
import org.osgi.service.event.Event;
import org.osgi.service.event.EventHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dexels.navajo.repository.api.AppStoreOperation;
import com.dexels.navajo.repository.api.RepositoryInstance;
import com.dexels.navajo.repository.api.RepositoryManager;
import com.dexels.navajo.tipi.dev.server.appmanager.AppStoreManager;
import com.dexels.navajo.tipi.dev.server.appmanager.operations.core.BaseOperation;

@Component(name = "tipi.dev.operation.cachepreload", immediate = true, property = { "osgi.command.scope=tipi",
        "osgi.command.function=buildpreload", "alias=/cachepreload", "name=cachepreload", "servlet-name=cachepreload",
        "repo=git","event.topics=repository/change" })
public class BuildCachePreload extends BaseOperation implements AppStoreOperation, Servlet,EventHandler {

	private static final long serialVersionUID = 4675519591066489420L;
	private final static Logger logger = LoggerFactory
			.getLogger(BuildCachePreload.class);
	
	@Activate
	public void activate(Map<String,Object> settings) {
	    try {
	        super.activate(settings);
	    } catch (Exception e){
	        logger.error("Caught exception on activating CacheBuild: {}", e);
	    }
	}
	
	@Deactivate
	public void deactivate() {
		super.deactivate();
	}
	
	@Reference(unbind="clearAppStoreManager",policy=ReferencePolicy.DYNAMIC)
	public void setAppStoreManager(AppStoreManager am) {
		super.setAppStoreManager(am);
	}

	public void clearAppStoreManager(AppStoreManager am) {
		super.clearAppStoreManager(am);
	}
	@Reference(cardinality=ReferenceCardinality.MULTIPLE,unbind="removeRepositoryInstance",policy=ReferencePolicy.DYNAMIC,target="(type=webstart)")
	public void addRepositoryInstance(RepositoryInstance a) {
		super.addRepositoryInstance(a);
		try {
			build(a);
		} catch (IOException e) {
			logger.warn("Building cache on initial failed.",e);
		}
	}

	public void removeRepositoryInstance(RepositoryInstance a) {
		super.removeRepositoryInstance(a);
	}
	
	@Reference(unbind="clearRepositoryManager",policy=ReferencePolicy.DYNAMIC)
	public void setRepositoryManager(RepositoryManager repositoryManager) {
		super.setRepositoryManager(repositoryManager);
	}

	public void clearRepositoryManager(RepositoryManager repositoryManager) {
		super.clearRepositoryManager(repositoryManager);
	}
	
	
	private void buildpreload(String name) throws IOException {
		RepositoryInstance as = applications.get(name);
		build(as);
	}
	
	private void buildpreload() throws IOException {
		for (RepositoryInstance a: applications.values()) {
			build(a);
		}
	}
	
	
	@Override
	protected void doGet(HttpServletRequest req, HttpServletResponse resp)
			throws ServletException, IOException {
		String val = req.getParameter("app");
		verifyAuthorization(req, resp);
		if(val!=null) {
			buildpreload(val);
		} else {
			buildpreload();
		}
		writeValueToJsonArray(resp.getOutputStream(),"cache preload build ok");

	}
	
	@Override
	public void build(RepositoryInstance a) throws IOException {
		if("webstart".equals(a.applicationType())) {
			createPreload( "tipi",a.getRepositoryFolder());
			createPreload( "resource",a.getRepositoryFolder());
		} else {
			logger.info("Skipping non-webstart repo: {}",a.getRepositoryName());
		}
	}

	private void createPreload(String resourceName, File appFolder) throws IOException {
		File inputFolder = new File(appFolder,resourceName);
        File compressedOutput = new File(appFolder, resourceName+".tgz");
        
        if(!inputFolder.exists()) {
        	logger.warn("No such folder: Will not create preload");
        	return;
        }
        try {
			compress(inputFolder, compressedOutput);
		} catch (NoSuchAlgorithmException e) {
			logger.error("Error: ", e);
		}
	}


	private void compress(File inputFolder, File compressedOutput) throws IOException, NoSuchAlgorithmException {

        FileOutputStream fos = new FileOutputStream(compressedOutput);
		MessageDigest messageDigest = MessageDigest.getInstance("MD5");

        TarArchiveOutputStream taos = new TarArchiveOutputStream(new GZIPOutputStream(new BufferedOutputStream(fos)));
        taos.setBigNumberMode(TarArchiveOutputStream.BIGNUMBER_STAR); 
        taos.setLongFileMode(TarArchiveOutputStream.LONGFILE_GNU);
        Properties digestProps = new Properties();
        addFilesToCompression(messageDigest, taos, inputFolder,inputFolder, ".",digestProps);
        
//        digestProps.store(System.err, "dump");
       
        File ff = new File(inputFolder,"digest.properties"); // File.createTempFile("digest", ".txt");
        FileWriter tmpDigest = new FileWriter(ff);
        digestProps.store(tmpDigest, "preload digest");
        tmpDigest.close();
        addFilesToCompression(null, taos, inputFolder, ff, ".", null);
        ff.delete();
        taos.close();

	}

	
	private String createDigest(MessageDigest digest, File file) throws IOException {
		OutputStream nullOutput = new OutputStream(){

			@Override
			public void write(int b) throws IOException {
			}};
		DigestOutputStream dos = new DigestOutputStream(nullOutput, digest);
		InputStream fis = null;
		try {
			fis = new FileInputStream(file);
			IOUtils.copy(fis,dos);
		} catch (FileNotFoundException e) {
			logger.error("Error: ", e);
		} finally {
			if(fis!=null) {
				try {
					fis.close();
				} catch (IOException e) {
				}
			}
		}
		
		final byte[] bytes = digest.digest();
		return new String(Base64.encodeBase64(bytes)).trim();
	}

    private void addFilesToCompression(MessageDigest digest,TarArchiveOutputStream taos,File root, File file, String dir, Properties digestProperties) throws IOException{

        if (file.isFile()) {
            taos.putArchiveEntry(new TarArchiveEntry(file, dir));
            BufferedInputStream bis = new BufferedInputStream(new FileInputStream(file));
            IOUtils.copy(bis, taos);
            taos.closeArchiveEntry();
            if(digest!=null) {
                String digestString = createDigest(digest, file);
//              logger.info("Creating digest for: "+file.getAbsolutePath()+" result: "+digestString);
              String key = relativePath(root, file);
              digestProperties.put(key, digestString);
            }
            bis.close();
//            taos.closeArchiveEntry();
        }

        else if(file.isDirectory()) {
            for (File childFile : file.listFiles()) {
                addFilesToCompression(digest,taos,root,  childFile, file.getName(),digestProperties);
            }
        }
    }
	

	
	private String relativePath(File base, File path) {
		return base.toURI().relativize(path.toURI()).getPath();
	}


	@Override
	public void handleEvent(Event event) {
		RepositoryInstance ri = (RepositoryInstance)event.getProperty("repository");
		Object autobuildobject = ri.getSettings().get("autobuild");
		if(autobuildobject instanceof Boolean) {
			Boolean b = (Boolean) autobuildobject;
			if(b==null || b==false) {
				logger.warn("Skipping build for non-autobuilding repo");
				return;
			}
		}
		if(autobuildobject instanceof String) {
			String b = (String) autobuildobject;
			if(!"true".equals(b)) {
				logger.warn("Skipping build for non-autobuilding repo");
				return;
			}
		}
		try {
			build(ri);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}


}
