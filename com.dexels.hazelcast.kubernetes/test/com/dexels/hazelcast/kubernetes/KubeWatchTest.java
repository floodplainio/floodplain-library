package com.dexels.hazelcast.kubernetes;

import java.io.IOException;

import org.junit.Ignore;
import org.junit.Test;

import com.dexels.kubernetes.watch.KubeWatch;

public class KubeWatchTest {
	
	// Used for some local test. Using this for structural testing would be quite a bit more involved.
	@Test @Ignore
	public void testKubeWatch() throws InterruptedException, IOException {
		KubeWatch kw = new KubeWatch();
		kw.activate();
		Thread.sleep(1000);
	}
}
