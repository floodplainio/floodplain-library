package com.dexels.hazelcast.test;

import java.io.IOException;
import java.util.Dictionary;

import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import com.dexels.hazelcast.impl.HazelcastTopologyConfiguration;
import com.hazelcast.core.Hazelcast;

public class HazelcastTopologyTest {

    @Test
    public void testClusterA() throws IOException {
        HazelcastTopologyConfiguration config = new HazelcastTopologyConfiguration();
        Dictionary<String, Object> d = config.createSettings("cluster-a", "test-cluster-a-2", null,
                HazelcastTopologyTest.class.getResourceAsStream("cluster_topology.xml"), 1234, "host1");
        String[] members = (String[]) d.get("cluster.members");
        Assert.assertEquals(3, members.length); // only members that are not me are added
        Assert.assertEquals("10.0.1.1", d.get("cluster.hostName"));

    }

    @Test
    public void testClusterB() throws IOException {
        HazelcastTopologyConfiguration config = new HazelcastTopologyConfiguration();
        Dictionary<String, Object> d = config.createSettings("cluster-b", "test-cluster-b-1", null,
                HazelcastTopologyTest.class.getResourceAsStream("cluster_topology.xml"), 1234, "host2");
        String[] members = (String[]) d.get("cluster.members");
        Assert.assertEquals(2, members.length);// only members that are not me are added
        Assert.assertEquals("10.0.1.2", d.get("cluster.hostName"));

    }
    
    @After
    public void cleanup() throws Exception {
        Hazelcast.shutdownAll(); 
    }
}
