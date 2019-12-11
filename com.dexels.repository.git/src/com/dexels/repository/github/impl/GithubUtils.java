package com.dexels.repository.github.impl;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.StreamSupport;

import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.HttpClientBuilder;
import org.eclipse.jgit.api.errors.GitAPIException;
import org.eclipse.jgit.diff.DiffEntry;
import org.eclipse.jgit.diff.DiffEntry.ChangeType;
import org.osgi.service.event.Event;
import org.osgi.service.event.EventAdmin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dexels.navajo.repository.api.diff.EntryChangeType;
import com.dexels.navajo.repository.api.diff.RepositoryChange;
import com.dexels.repository.git.GitRepository;
import com.dexels.repository.git.impl.GitRepositoryInstanceImpl;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class GithubUtils {

	
	private final static Logger logger = LoggerFactory.getLogger(GithubUtils.class);

    public static void deregisterCallbackUrl(String reponame, Integer callbackUrlId) {
        logger.info("Going to de-register our webhook on {} with ID: {}", reponame, callbackUrlId);
        setProxySystemProperties();
        
        if (callbackUrlId == null) {
            return;
        }
        if (reponame == null) {
            logger.error("Unable to deregister callback url due to missing reponame");
            return;
        }

        HttpClient client = HttpClientBuilder.create().build();
        HttpDelete delete = new HttpDelete("https://api.github.com/repos/dexels/" + reponame + "/hooks/" + callbackUrlId);

        try {

            HttpResponse response = client.execute(delete);
            BufferedReader rd = new BufferedReader(new InputStreamReader(response.getEntity().getContent()));

            int responseCode = response.getStatusLine().getStatusCode();
            if (responseCode == 204) {
                logger.info("Successfully de-registered our webhook on {} with ID: {}", reponame, callbackUrlId);
            } else {
                StringBuffer result = new StringBuffer();
                String line = "";

                while ((line = rd.readLine()) != null) {
                    result.append(line);
                }

                logger.warn("Error on de-registering callback url on {} with ID {}. Got code: {}, message: {}",
                        reponame, callbackUrlId, responseCode, result);
            }

        } catch (Exception e) {
            logger.error("Exception in de-registering GitHub callback! {} ", e);
        }

    }

    public static void setProxySystemProperties() {
    	setProxyEnvToSystemProperty("httpProxyPort", "http.proxyHost");
    	setProxyEnvToSystemProperty("httpProxyHost", "http.proxyPort");
    }
	
    private static void setProxyEnvToSystemProperty(String envKey, String systemPropertyKey) {
	   String env = System.getenv(envKey);
	   if(env!=null) {
		   System.setProperty(systemPropertyKey, env);
	   }
    }

    public static boolean hookexists(String reponame,String callbackUrl,String token, String organization) throws ClientProtocolException, IOException {
    	ObjectMapper objectMapper = new ObjectMapper();
    	setProxySystemProperties();
        HttpClient client = HttpClientBuilder.create().useSystemProperties().build();
        HttpGet get = new HttpGet("https://api.github.com/repos/"+organization+"/" + reponame + "/hooks?access_token=" + token);
        HttpEntity entity = client.execute(get).getEntity();
        JsonNode jn = objectMapper.readTree(entity.getContent());
        StringWriter sw = new StringWriter();
        objectMapper.writerWithDefaultPrettyPrinter().writeValue(sw, jn);
        return StreamSupport.stream(jn.spliterator(), false)
        	.map(e->(ObjectNode)e)
        	.map(e->(ObjectNode)e.get("config"))
        	.map(e->e.get("url").asText())
        	.filter(e->e.equals(callbackUrl))
        	.findAny()
        	.isPresent();
    }

    public static Integer registerCallbackUrl(String reponame,String callbackUrl,String token, String organization, String hookType) throws ClientProtocolException, IOException {
    	logger.info("Registering callback for repo: "+reponame+" callback: "+callbackUrl+" token: ****");
        setProxySystemProperties();
    	System.err.println("Registering callback for repo: "+reponame+" callback: "+callbackUrl+" token: ****");
        if (callbackUrl == null || callbackUrl.isEmpty()) {
            return null;
        }
        
        if (reponame == null) {
            logger.error("Unable to register callback url due to missing reponame");
            return null;
        }
        logger.info("Checking if callback URL to {} already exists: ", callbackUrl);
        if(hookexists(reponame, callbackUrl, token, organization)) {
            logger.info("Hook exists. Ignoring. ");
        	return null;
        }
        logger.info("Going to register a GitHub callback URL to {}", callbackUrl);
        HttpClient client = HttpClientBuilder.create().useSystemProperties().build();
        HttpPost post = new HttpPost("https://api.github.com/repos/"+organization+"/" + reponame + "/hooks?access_token=" + token);

        Map<String, String> config = new HashMap<String, String>();
        config.put("url", callbackUrl);
        if(!hookType.equals("form") && !hookType.equals("json")) {
        	throw new IllegalArgumentException("Illegal hook type: "+hookType+" It should be json or form");
        }
        config.put("content_type", hookType);

        Map<String, Object> payloadMap = new HashMap<String, Object>();
        payloadMap.put("name", "web");
        payloadMap.put("active", true);
        payloadMap.put("events", new String[] { "push" });
        payloadMap.put("config", config);

        String jsonPayloag = getJson(payloadMap);

        StringEntity requestEntity = new StringEntity(jsonPayloag, ContentType.APPLICATION_JSON);

        try {
            post.setEntity(requestEntity);

            HttpResponse response = client.execute(post);
            BufferedReader rd = new BufferedReader(new InputStreamReader(response.getEntity().getContent()));

            StringBuffer result = new StringBuffer();
            String line = "";

            while ((line = rd.readLine()) != null) {
                result.append(line);
            }

            int responseCode = response.getStatusLine().getStatusCode();
            if (responseCode == 201) {
                Header locationHeader = response.getHeaders("Location")[0];
                logger.debug("GitHub callback URL registered successfully!");
                // We expect something like https://api.github.com/repos/user/repo/hooks/1
                if (locationHeader.getValue().indexOf("/") < 0) {
                    logger.warn("Unepexected reply from Github - unable to save callback ID. We will never de-register!");
                } else {
                    String id = locationHeader.getValue().substring(locationHeader.getValue().lastIndexOf("/") + 1);
//                    callbackUrlId = Integer.valueOf(id);
                    logger.info("GitHub callback URL id: {}", id);
                    return Integer.valueOf(id);
                }

                logger.info("GitHub callback URL register complete");

            } else {
                logger.warn("Error on registering callback url. Got code: {}, message: {}", responseCode, result);
            }

        } catch (Exception e) {
            logger.error("Exception in registering GitHub callback! {} ", e);
        }
        return null;
    }

    private static String getJson(Object obj) {
        String res = null;
        try {
            res = new ObjectMapper().writerWithDefaultPrettyPrinter().writeValueAsString(obj);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return res;
    }


    public static void checkForCommitChanges(GitRepositoryInstanceImpl instance, EventAdmin eventAdmin, final String newVersion, final String currentCommitVersion) throws IOException, GitAPIException {
        
        if (newVersion!=null && newVersion.equals(currentCommitVersion)) {
            logger.debug("Identical versions. Nothing pulled");
            return;
        }
        if (currentCommitVersion == null) {
            logger.info("weird, no known current commit known?...");
//            currentCommitVersion = getLastCommitVersion();
            return;
        }
        
        
        Map<String, Object> properties = fileChangesBetweenVersions(instance, currentCommitVersion, newVersion);
        sendChangeEvent(instance,eventAdmin,RepositoryChange.TOPIC, properties);
    }



	public static Map<String, Object> fileChangesBetweenVersions(GitRepositoryInstanceImpl instance,
			final String currentCommitVersion, final String newVersion) throws IOException {

        Map<String, Object> properties = new HashMap<String, Object>();

		List<DiffEntry> diffEntries;
		try {
			diffEntries = instance.diff(currentCommitVersion);
		} catch (GitAPIException e) {
			throw new IOException("Git related error",e);
		}

        if (diffEntries.isEmpty()) {
            logger.info("Empty changeset (but there was a commit). Maybe empty commit?");
            return properties;
        }

        List<String> added = new ArrayList<String>();
        List<String> modified = new ArrayList<String>();
        List<String> copied = new ArrayList<String>();
        List<String> deleted = new ArrayList<String>();

        
        for (DiffEntry diffEntry : diffEntries) {

            if (diffEntry.getChangeType().equals(ChangeType.ADD)) {
                added.add(diffEntry.getNewPath());
            } else if (diffEntry.getChangeType().equals(ChangeType.MODIFY)) {
                modified.add(diffEntry.getNewPath());
            } else if (diffEntry.getChangeType().equals(ChangeType.COPY)) {
                copied.add(diffEntry.getOldPath());
            } else if (diffEntry.getChangeType().equals(ChangeType.DELETE)) {
                deleted.add(diffEntry.getOldPath());
            } else if (diffEntry.getChangeType().equals(ChangeType.RENAME)) {
                added.add(diffEntry.getNewPath());
                deleted.add(diffEntry.getOldPath());
            }

        }
        properties.put(EntryChangeType.ADD.name(), added);
        properties.put(EntryChangeType.MODIFY.name(), modified);
        properties.put(EntryChangeType.COPY.name(), copied);
        properties.put(EntryChangeType.DELETE.name(), deleted);
        if (currentCommitVersion != null) {
            properties.put("oldCommit", currentCommitVersion);
        }
        properties.put("newCommit", newVersion);
        String url = instance.getUrl();
        if (url != null) {
            properties.put("url", url);
        }
		return properties;
	}
    
    private static void sendChangeEvent(final GitRepository instance, final EventAdmin eventAdmin, final String topic, final Map<String, Object> properties) {
        properties.put("repository", instance);
        properties.put("repositoryName", instance.getRepositoryName());
        Event event = new Event(topic, properties);
        eventAdmin.postEvent(event);

    }
    
    public static String extractHttpUriFromGitUri(String githubUri, String reponame) {
        // githubUri = git@github.com:user/project-name.git
        // httpUri = https://github.com/user/project-name
        if (githubUri.indexOf(":") < 1) {
            logger.error("Unexepcted format of Git URI - cannot extract Http uri!");
            return "";
        }
        if (githubUri.contains("https")) {
            // already in right format
            return githubUri;
        }

        String userProject = githubUri.split(":")[1];
        String user = userProject.substring(0, userProject.indexOf('/'));
        return "https://github.com/" + user + "/" + reponame;
    }
    
    public static String extractGitHubRepoNameFromuri(String githubUri) {
        if (githubUri.contains("https")) {
            // already in right format
            String withSuffix = githubUri.substring(githubUri.lastIndexOf("/") + 1, githubUri.length());
            return withSuffix.endsWith(".git")?withSuffix.substring(0, withSuffix.length()-4):withSuffix;
        }
        String userProject = githubUri.split(":")[1];
        userProject = userProject.substring(userProject.indexOf("/")+1);
        String withoutGit = userProject.endsWith(".git")?userProject.substring(0, userProject.length()-4):userProject;
		return withoutGit;
    }
    
    public static String extractUserFromGitHubURI(String githubUri) {
        if (githubUri.contains("https")) {
//             already in right format
//            return githubUri.substring(githubUri.lastIndexOf("/") + 1, githubUri.length());
            String[] split = githubUri.split("/");
            if(split.length<3) {
            	return null;
            }
            String username = split[split.length-2];
            return username;
        }
        String userProject = githubUri.split(":")[1];
        return userProject.substring(0, userProject.indexOf('/'));
    }
    
    public static void main(String[] args) {
    }
}
