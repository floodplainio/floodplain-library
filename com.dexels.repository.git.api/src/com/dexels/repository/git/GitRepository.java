package com.dexels.repository.git;

import java.io.File;

public interface GitRepository {

	public String getUrl();

	public String getRepositoryName();

	public File getGitFolder();

	public String getCurrentCommit();

}
