package jnlp.sample.servlet.impl;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;

import org.osgi.service.component.annotations.Activate;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.ConfigurationPolicy;
import org.osgi.service.component.annotations.Deactivate;
import org.osgi.service.component.annotations.Reference;
import org.osgi.service.component.annotations.ReferenceCardinality;
import org.osgi.service.component.annotations.ReferencePolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dexels.navajo.repository.api.RepositoryInstance;
import com.dexels.navajo.repository.api.RepositoryManager;

import jnlp.sample.servlet.ResourceResolver;

@Component(name="tipi.dev.resourceresolver",configurationPolicy=ConfigurationPolicy.IGNORE, immediate=true)
public class FileSystemResourceResolver implements ResourceResolver {
	private static final String JNLP_MIME_TYPE = "application/x-java-jnlp-file";

	private static final String JAR_MIME_TYPE_NEW = "application/java-archive";

	
	private final static Logger logger = LoggerFactory.getLogger(FileSystemResourceResolver.class);

	// Default extension for the JNLP file
	private static final String JNLP_EXTENSION = ".jnlp";
	private static final String JAR_EXTENSION = ".jar";
	private File baseDir;
	private final String basePath = "/apps/";

	private Map<String,RepositoryInstance> repositories = new HashMap<>();

	private RepositoryManager repositoryManager;

	public FileSystemResourceResolver() {
	}

	@Activate
	public void activate() {
		this.baseDir = repositoryManager.getRepositoryFolder();
	}
	
	@Deactivate
	public void deactivate() {
		this.baseDir = null;
	}
	
	public void setBaseDir(File baseDir) {
		this.baseDir = baseDir;
	}
	
	@Reference(unbind="removeRepository",policy=ReferencePolicy.DYNAMIC,target="(repository.type=webstart)",cardinality=ReferenceCardinality.MULTIPLE)
	public void addRepository(RepositoryInstance instance) {
		repositories.put(instance.getRepositoryName(), instance);
	}

	public void removeRepository(RepositoryInstance instance) {
		repositories.remove(instance.getRepositoryName());
	}
	
	@Reference(unbind="clearRepositoryManager",policy=ReferencePolicy.DYNAMIC)
	public void setRepositoryManager(RepositoryManager repositoryManager) {
		this.repositoryManager = repositoryManager;
	}
	
	public void clearRepositoryManager(RepositoryManager repositoryManager) {
		this.repositoryManager = null;
	}
	
	
	@Override
	public String getMimeType(String path) {
		if (path.endsWith(JNLP_EXTENSION)) {
			return JNLP_MIME_TYPE;
		}
		if (path.endsWith(JAR_EXTENSION)) {
			return JAR_MIME_TYPE_NEW;
		}
		return null;
	}

	@Override
	public URL getResource(String path) throws IOException {
//		String realPath = path.substring("Apps/".length(), path.length());
		// Thread.dumpStack();
//		/apps/env.managed.repository-master/etc/dexels.bundles.cfg
		
		String repo = repositoryNameFromPath(path);
		if(repo==null) {
			logger.error("Path resolve failed for path: {}: No repo found",path);
			throw new IOException("Repo error");
		}
		RepositoryInstance ri = repositories.get(repo);
		if(ri==null) {
			logger.error("Path resolve failed for path: {}  repository {} is missing",path,repo);
			throw new IOException("Repo error.");
		}
		String internalPath = pathWithoutRepository(path);
		boolean isPublic = isPublic(internalPath);
		if(!isPublic) {
			logger.error("Path resolve denied for path: {}  repository: {} internal path: {}",path,repo,internalPath);
			throw new IOException("Repo error..");
		}
		File actual = new File(ri.getRepositoryFolder(),internalPath);
		System.err.println("Actual path: "+actual.getAbsolutePath());
		return actual.toURI().toURL();
		
//		String realPath = path.substring(basePath.length(), path.length());
//		File result = new File(baseDir, realPath);
//		return result.toURI().toURL();
	}

	private String repositoryNameFromPath(String path) throws FileNotFoundException {
		String realPath = path.substring(basePath.length(), path.length());
		int firstSlash = realPath.indexOf('/');
		if(firstSlash==-1) {
			throw new FileNotFoundException("No such part");
		}
		String repName = realPath.substring(0, firstSlash);
		return repName;
	}
	
	private String pathWithoutRepository(String path) throws FileNotFoundException {
		String realPath = path.substring(basePath.length(), path.length());
		int firstSlash = realPath.indexOf('/');
		if(firstSlash==-1) {
			throw new FileNotFoundException("No such part");
		}
		String pathWithout = realPath.substring(firstSlash+1, realPath.length());
		return pathWithout;
	}
	
	private boolean isPublic(String repositoryPath) {
		return repositoryPath.startsWith("tipi") || repositoryPath.startsWith("resource") || repositoryPath.startsWith("lib") || repositoryPath.indexOf("jnlp")!=-1;
	}
	
	@Override
	public File getDir(String dirPath) {
		String realPath = dirPath.substring(basePath.length(), dirPath.length());
		return new File(baseDir, realPath);
	}

	@Override
	public long getLastModified(String path) {
		String realPath = path.substring(basePath.length(), path.length());
		File result = new File(baseDir, realPath);
		if (!result.exists()) {
			// logger.info("File with path: "+path+" resolved to: "+result.getAbsolutePath()+" does not seem to exist!");
		}
		return result.lastModified();
		// return 0;
	}
	
	
	public static void main(String[] args) throws FileNotFoundException {
		new FileSystemResourceResolver().test();

	}

	private void test() throws FileNotFoundException {
		String path = "/apps/env.managed.repository-master/etc/dexels.bundles.cfg";
		String path2 = "/apps/club-test/knvb.jnlp";
		String path3 = "/apps/club-test/lib/_3/com.dexels.navajo.tipi.swing.substance_1.2.0__V1.2.0.jar";
		String path4 = "/apps/backoffice-production/lib/_1/flute_1.3.jar";
		
		
		
		String repname =  repositoryNameFromPath(path);
		String pth =  pathWithoutRepository(path);
		boolean publicRep =  isPublic(pth);

		String repname2 =  repositoryNameFromPath(path2);
		String pth2 =  pathWithoutRepository(path2);
		boolean publicRep2 =  isPublic(pth2);

		String repname3 =  repositoryNameFromPath(path3);
		String pth3 =  pathWithoutRepository(path3);
		boolean publicRep3 =  isPublic(pth3);

		String repname4 =  repositoryNameFromPath(path4);
		String pth4 =  pathWithoutRepository(path4);
		boolean publicRep4 =  isPublic(pth4);
		
		System.err.println("Rep: "+repname+" internal: "+ pth +" publicRep: "+publicRep);
		System.err.println("Rep: "+repname2+" internal: "+ pth2 + " publicRep: "+publicRep2);
		System.err.println("Rep: "+repname3+" internal: "+ pth3 +" publicRep: "+publicRep3);
		System.err.println("Rep: "+repname4+" internal: "+ pth4 +" publicRep: "+publicRep4);
	}
}
