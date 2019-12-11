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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.ResourceBundle;

import javax.servlet.Servlet;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.compress.archivers.zip.ZipArchiveEntry;
import org.apache.commons.compress.archivers.zip.ZipArchiveOutputStream;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.osgi.service.component.annotations.Component;
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
import com.dexels.navajo.tipi.dev.core.projectbuilder.Dependency;
import com.dexels.navajo.tipi.dev.core.projectbuilder.LocalJnlpBuilder;
import com.dexels.navajo.tipi.dev.core.sign.SignComponent;
import com.dexels.navajo.tipi.dev.server.appmanager.AppStoreManager;
import com.dexels.navajo.tipi.dev.server.appmanager.impl.RepositoryInstanceWrapper;
import com.dexels.navajo.tipi.dev.server.appmanager.impl.UnsignJarTask;
import com.dexels.navajo.tipi.dev.server.appmanager.operations.core.BaseOperation;

@Component(name="tipi.dev.operation.build", immediate=true,service={Servlet.class,AppStoreOperation.class,JnlpBuild.class,EventHandler.class},property={"osgi.command.scope=tipi","osgi.command.function=build", "alias=/build","name=build","servlet-name=build","type=webstart","event.topics=repository/change"})
public class JnlpBuild extends BaseOperation implements AppStoreOperation,Servlet,EventHandler {
	
	private static final long serialVersionUID = -325075211700621696L;
	private static final Logger logger = LoggerFactory.getLogger(JnlpBuild.class);
	private SignComponent signComponent;
	
	
	@Reference(unbind="clearAppStoreManager",policy=ReferencePolicy.DYNAMIC)
	@Override
	public void setAppStoreManager(AppStoreManager am) {
		super.setAppStoreManager(am);
	}

	@Override
	public void clearAppStoreManager(AppStoreManager am) {
		super.clearAppStoreManager(am);
	}
	
	@Override
	@Reference(cardinality=ReferenceCardinality.MULTIPLE,unbind="removeRepositoryInstance",policy=ReferencePolicy.DYNAMIC)
	public void addRepositoryInstance(RepositoryInstance a) {
		super.addRepositoryInstance(a);
	}

	@Override
	public void removeRepositoryInstance(RepositoryInstance a) {
		super.removeRepositoryInstance(a);
	}
	
	@Override
	@Reference(unbind="clearRepositoryManager",policy=ReferencePolicy.DYNAMIC)
	public void setRepositoryManager(RepositoryManager repositoryManager) {
		super.setRepositoryManager(repositoryManager);
	}

	@Override
	public void clearRepositoryManager(RepositoryManager repositoryManager) {
		super.clearRepositoryManager(repositoryManager);
	}	

	
	public void build(String name) throws IOException {
		RepositoryInstance as = applications.get(name);
		if(as==null) {
			for (RepositoryInstance a: applications.values()) {
				build(a);
			}
			
		} else {
			build(as);
		}
	}

    private void buildDigest(RepositoryInstance a) throws IOException {
        if(! "webstart".equals(a.applicationType())) {
            return;
        }
        
        MessageDigest messageDigest;
        try {
            messageDigest = MessageDigest.getInstance("MD5");
            createDigestFor(messageDigest, "tipi", a.getRepositoryFolder());
            createDigestFor(messageDigest, "resource", a.getRepositoryFolder());
        } catch (NoSuchAlgorithmException e) {
            logger.error("NoSuchAlgorithmException: ", e);
        }
    }

	@Override
	protected void doGet(HttpServletRequest req, HttpServletResponse resp)
			throws ServletException, IOException {
		try {
			verifyAuthorization(req, resp);
			String val = req.getParameter("app");
			if(val!=null) {
				build(val);
			} else {
				build();
			}
			writeValueToJsonArray(resp.getOutputStream(),"build ok");
		} catch (Exception e) {
			logger.error("Server Error: ", e);
			try {
				resp.sendError(500,"Server issue");
			} catch (Exception e1) {
				logger.error("Error: ", e1);
			}
		}
		
	}
	
	@Reference(unbind="clearSignComponent",policy=ReferencePolicy.DYNAMIC,service=SignComponent.class)
	public void setSignComponent(SignComponent signComponent) {
		this.signComponent = signComponent;
	}
	
	public void clearSignComponent(SignComponent signComponent) {
		this.signComponent = null;
	}
	
	public void build() {
		for (RepositoryInstance a: applications.values()) {
			try {
				build(a);
			} catch (IOException e) {
				logger.error("Error: ", e);
			}
		}
	}	
	
	public synchronized void build(RepositoryInstance repoInstance, boolean resourcesOnly) throws IOException {
        buildDigest(repoInstance);
        RepositoryInstanceWrapper a = new RepositoryInstanceWrapper(repoInstance);
        a.load();

        if (resourcesOnly) {
            for (String profile : a.getProfiles()) {
               createSignedResourcesJar(repoInstance.getRepositoryFolder(), profile);
            }
            return;
        }
        
        List<String> extraHeaders = new ArrayList<>();
        String securityHeader = null;
        if ( a.getSettingsBundle() != null &&  a.getSettingsBundle().containsKey("permissions")) {
            securityHeader = a.getSettingsBundle().getString("permissions");
        }
        if (securityHeader != null) {
            if ("all".equals(securityHeader)) {
                extraHeaders.add("Permissions: all-permissions");
            }
            if ("j2ee".equals(securityHeader)) {
                extraHeaders.add("Permissions: j2ee-application-client-permissions");
            }
            if ("none".equals(securityHeader)) {
                extraHeaders.add("Permissions: sandbox");
            }
        } else {
            extraHeaders.add("Permissions: all-permissions");
        }
       
       
        String applicationName = appStoreManager.getApplicationName();
        if(applicationName!=null) {
            extraHeaders.add("Application-Name: "+applicationName);
        }
        String manifestCodebase = appStoreManager.getManifestCodebase();
        if(manifestCodebase!=null) {
            extraHeaders.add("Codebase: "+manifestCodebase);
        } else {
            extraHeaders.add("Codebase: *");
        }
        
        Map<String, Object> settings = repoInstance.getSettings();
        String masterrevision = settings!=null ? (String) settings.get("masterrevision") : null;
        
        
        File unsigned = new File(repoInstance.getRepositoryFolder(), "unsigned");

        if(unsigned.exists()) {
            FileUtils.deleteDirectory(unsigned);
        }
        if (!unsigned.exists()) {
            unsigned.mkdirs();
        }
        
        File repo = new File(getRepositoryManager().getOutputFolder(), "repo");
        File lib = null;
        if (masterrevision!=null) {
            File baselib =new File(repoInstance.getRepositoryFolder(), "lib");
            lib = new File(baselib,"_"+masterrevision);
        } else {
            lib =new File(repoInstance.getRepositoryFolder(),"lib");
        }
        if(lib.exists()) {
            FileUtils.deleteDirectory(lib);
        }
        if(!lib.exists()) {
            lib.mkdirs();
        }
        
        
        for (Dependency dd : a.getDependencies()) {
            File localSigned = dd.getFilePathForDependency(repo, securityHeader);
            if(!localSigned.exists()) {
                UnsignJarTask.downloadDepencency(dd,repo, new File(unsigned.getAbsolutePath()), securityHeader, extraHeaders);
                signdependency(dd, securityHeader, repo);
                
            }
            FileUtils.copyFileToDirectory(localSigned, lib );

        }

        LocalJnlpBuilder jj = new LocalJnlpBuilder();
        jj.buildFromMaven(a.getSettingsBundle(),a.getDependencies(),repoInstance.getRepositoryFolder(),a.getProfiles(),"",appStoreManager.getCodeBase(),repoInstance.getRepositoryName(),repoInstance.getDeployment(), settings);
        for (String profile : a.getProfiles()) {
            createSignedJnlpJar(a.getSettingsBundle(),repoInstance.getRepositoryFolder(),profile);
        }
	}
	
	@Override
	public synchronized void build(RepositoryInstance repoInstance) throws IOException {
	    build(repoInstance, false);
	}
	
    private void createSignedResourcesJar(File repositoryFolder, String profile) {
        File tipi = new File(repositoryFolder, "tipi");
        File resource = new File(repositoryFolder, "resource");
        
        File libFolder = new File(repositoryFolder, "lib");
        if (!libFolder.exists()) {
            libFolder.mkdirs();
        }
        signFolderCached(tipi, profile, libFolder);
        signFolderCached(resource, profile, libFolder);
        
    }


    private void createSignedJnlpJar(ResourceBundle settingsBundle, File repositoryFolder, String profile) throws IOException {
         File f = new File(repositoryFolder, "tmp");
        f.mkdir();
        File metaInf = new File(f, "META-INF");
        metaInf.mkdirs();
        File manifest = new File(metaInf, "MANIFEST.MF");
        try(FileWriter maniWriter = new FileWriter(manifest)) {
        

        if (settingsBundle != null && settingsBundle.containsKey("permissions")) {
            if ("all".equals(settingsBundle.getString("permissions"))) {
                maniWriter.write("Permissions: all-permissions\r\n");
            }
            if ("j2ee".equals(settingsBundle.getString("permissions"))) {
                maniWriter.write("Permissions: j2ee-application-client-permissions\r\n");
            }
            if ("none".equals(settingsBundle.getString("permissions"))) {
                maniWriter.write("Permissions: sandbox\r\n");
            }
        } else {
            maniWriter.write("Permissions: all-permissions\r\n");
        }
	        maniWriter.write("Codebase: *\r\n");
	        maniWriter.write("Application-Name: " + profile + "\r\n");
        }

        File jnlpInf = new File(f, "JNLP-INF");
        jnlpInf.mkdirs();
        File jnlpFile = new File(repositoryFolder, profile + ".jnlp");
        FileUtils.copyFile(jnlpFile, new File(jnlpInf, "APPLICATION.JNLP"));
        File libFolder = new File(repositoryFolder, "lib");
        if (!libFolder.exists()) {
            libFolder.mkdirs();
        }
        
        signJnlp(jnlpFile, profile, libFolder);
        
        createSignedResourcesJar(repositoryFolder, profile);
    }
	
	@SuppressWarnings("unused")
    private void addFolderToJar(File folder,
			ZipArchiveOutputStream jarOutputStream, File jarFile,
			String baseName) throws IOException {					
		File[] files = folder.listFiles();
		for (File file : files) {
			if (file.isDirectory()) {
				addFolderToJar(file, jarOutputStream, jarFile, baseName);
			} else {
				String name = file.getAbsolutePath().substring(baseName.length());
				ZipArchiveEntry zipEntry = new ZipArchiveEntry(name);
				jarOutputStream.putArchiveEntry(zipEntry);
				FileInputStream input = new FileInputStream(file);
				IOUtils.copy(input, jarOutputStream);
				input.close();
				jarOutputStream.closeArchiveEntry();
			}
		}

	}

	private void signJnlp(File jnlpFile, String profile, File baseDir) {
		if(signComponent==null) {
			logger.warn("Can not sign jnlp file: Missing sign component");
			return;
		}
		signComponent.signJnlp(jnlpFile, profile,  baseDir);
	}
	
	private void signFolderCached(File folder, String profile, File baseDir) {
        if(signComponent==null) {
            logger.warn("Can not sign jnlp file: Missing sign component");
            return;
        }
        signComponent.signDirectoryCached(folder, profile, baseDir);
    }

	private void signdependency(Dependency d, String securityHeader, File repo) {
		if(signComponent==null) {
			logger.warn("Can not sign jar file: Missing sign component");
			return;
		}
		signComponent.signdependency(d, securityHeader, repo);
	}

	@SuppressWarnings("unchecked")
	public void handleEvent(Event event) {
	    logger.info("Received event: {}", event.getTopic());
		RepositoryInstance ri = (RepositoryInstance)event.getProperty("repository");
		Object autobuildobject = ri.getSettings().get("autobuild");
		if(autobuildobject instanceof Boolean) {
			Boolean b = (Boolean) autobuildobject;
			if(!b) {
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
		List<String> added = (List<String>) event.getProperty("ADD");
		List<String> modified = (List<String>) event.getProperty("MODIFY");
		List<String> copied = (List<String>) event.getProperty("COPY");
		List<String> deleted = (List<String>) event.getProperty("DELETE");
		List<String> all = new ArrayList<String>();
		all.addAll(added);
		all.addAll(copied);
		all.addAll(modified);
		all.addAll(deleted);
		
		boolean modifiedSettings = false;
		boolean modifiedCache = false;
		for (String path : all) {
			if(path.startsWith("settings/")) {
				modifiedSettings = true;
			} else if (path.startsWith("tipi/") || path.startsWith("resource/")) {
			    modifiedCache = true;
			}
		}
		logger.info("Builder detected {} changes. ",all.size());
		if(modifiedSettings) {
			logger.info("Settings change detected, building JNLP");
			new Thread( new Runnable() {

                @Override
                public void run() {
                    try {
                        build(ri);
                    } catch (IOException e) {
                        logger.error("Error: ", e);
                    }
                    logger.info("Finished building");
                }
			}).start();
			
		} else if (modifiedCache) {
		    logger.info("Tipi change detected, building cache");
		    new Thread( new Runnable() {

                @Override
                public void run() {
                    try {
                        build(ri, true); 
                    } catch (IOException e) {
                        logger.error("Error: ", e);
                    }
                    logger.info("Finished building");
                }
            }).start();
		} else {
		    logger.info("Skipping build, none of these changes were relevant: {}",all);
		}
		logger.info("Finished handling event");
		

	}
	
	private void createDigestFor(MessageDigest digest,String resourceName, File appFolder) throws IOException {
        File inputFolder = new File(appFolder,resourceName);
        File remoteDigestOutput = new File(inputFolder, "remotedigest.properties");
        if(!inputFolder.exists()) {
            logger.warn("No such folder: Will not create digest for: {}", inputFolder);
            return;
        }
        Properties properties = new Properties();
        scan(digest,inputFolder,inputFolder,properties);
        FileOutputStream fos = null;
        try {
            fos = new FileOutputStream(remoteDigestOutput);
            properties.store(fos, "Generated by Tipi Store");
        } catch (IOException e) {
            logger.error("Error: ", e);
        } finally {
            if(fos!=null) {
                try {
                    fos.close();
                } catch (IOException e) {
                }
            }
        }
    }
	
    private void scan(MessageDigest digest, File root, File current, Properties properties) throws IOException {
        File[] elements = current.listFiles();
        if (elements != null) {
            for (File file : elements) {
                if (file.isDirectory()) {
                    if (!"CVS".equals(file.getName())) {
                        scan(digest, root, file, properties);
                    }
                } else {
                    String mdigest = createDigest(digest, file).trim();
                    properties.put(relativePath(root, file), mdigest);
                }
            }
        }

    }

    private String createDigest(MessageDigest digest, File file) throws IOException {
        OutputStream nullOutput = new OutputStream() {

            @Override
            public void write(int b) throws IOException {
            }
        };
        DigestOutputStream dos = new DigestOutputStream(nullOutput, digest);
        InputStream fis = null;
        try {
            fis = new FileInputStream(file);
            copyResource(dos, fis);
        } catch (FileNotFoundException e) {
            logger.error("Error: ", e);
        } finally {
            if (fis != null) {
                try {
                    fis.close();
                } catch (IOException e) {
                }
            }
        }

        final byte[] bytes = digest.digest();
        return new String(Base64.encodeBase64(bytes));
    }

    private String relativePath(File base, File path) {
        return base.toURI().relativize(path.toURI()).getPath();
    }

    private final void copyResource(OutputStream out, InputStream in) throws IOException {
        BufferedInputStream bin = new BufferedInputStream(in);
        BufferedOutputStream bout = new BufferedOutputStream(out);
        byte[] buffer = new byte[1024];
        int read;
        while ((read = bin.read(buffer)) > -1) {
            bout.write(buffer, 0, read);
        }
        bin.close();
        bout.flush();
        bout.close();
    }



}
