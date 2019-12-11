package com.dexels.repository.git;

import java.io.IOException;
import java.util.Map;

import com.dexels.navajo.repository.api.RepositoryInstance;


public interface GitRepositoryInstance extends RepositoryInstance, GitRepository {

	public void callClone() throws IOException;

	public void callPull() throws IOException;


	public void callCheckout(String objectId, String branchname) throws IOException;

	public String getLastCommitVersion();

	public void callClean() throws IOException;


	public Map<String, Object> fileChangesBetweenVersions(String currentCommitVersion, String newVersion)
			throws IOException;


}