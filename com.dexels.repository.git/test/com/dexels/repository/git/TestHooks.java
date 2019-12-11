package com.dexels.repository.git;

import java.io.IOException;

import org.apache.http.client.ClientProtocolException;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import com.dexels.repository.github.impl.GithubUtils;


public class TestHooks {
	@Test @Ignore
	public void testHookExistence() throws ClientProtocolException, IOException {
		String token = "....";
		boolean b = GithubUtils.hookexists("sportlink", "http://url", token, "org");
		Assert.assertTrue(b);
		boolean c = GithubUtils.hookexists("sportlink", "http://url", token, "org");
		Assert.assertFalse(c);

	}
}
