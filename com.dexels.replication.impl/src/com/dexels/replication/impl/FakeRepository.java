package com.dexels.replication.impl;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.ConfigurationPolicy;

import com.dexels.navajo.repository.api.AppStoreOperation;
import com.dexels.navajo.repository.api.RepositoryInstance;

@Component(name="dexels.replication.fakerepo",enabled=true,configurationPolicy=ConfigurationPolicy.REQUIRE)
public class FakeRepository implements RepositoryInstance {

	@Override
	public int compareTo(RepositoryInstance o) {
		return 0;
	}

	@Override
	public File getRepositoryFolder() {
		return null;
	}

	@Override
	public File getTempFolder() {
		return null;
	}

	@Override
	public File getOutputFolder() {
		return null;
	}

	@Override
	public String getRepositoryName() {
		return null;
	}

	@Override
	public Map<String, Object> getSettings() {
		return null;
	}

	@Override
	public void addOperation(AppStoreOperation op, Map<String, Object> settings) {
		
	}

	@Override
	public void removeOperation(AppStoreOperation op, Map<String, Object> settings) {
	}

	@Override
	public List<String> getOperations() {
		return null;
	}

	@Override
	public void refreshApplication() throws IOException {
	}

	@Override
	public void refreshApplicationLocking() throws IOException {
	}

	@Override
	public String repositoryType() {
		return null;
	}

	@Override
	public String applicationType() {
		return null;
	}

	@Override
	public String getDeployment() {
		return "test";
	}

	@Override
	public Set<String> getAllowedProfiles() {
		return null;
	}

	@Override
	public Map<String, Object> getDeploymentSettings(Map<String, Object> source) {
		return null;
	}

	@Override
	public boolean requiredForServerStatus() {
		return false;
	}

}
