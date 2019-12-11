package com.dexels.repository.github;

import com.dexels.repository.git.GitRepositoryInstance;


public interface GitHubRepositoryInstance extends GitRepositoryInstance {
    public String getHttpUrl();
}