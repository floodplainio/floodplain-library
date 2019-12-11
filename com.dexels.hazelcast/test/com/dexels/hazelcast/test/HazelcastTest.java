package com.dexels.hazelcast.test;

import java.util.Map;

import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import com.dexels.hazelcast.HazelcastService;
import com.hazelcast.core.Hazelcast;

public class HazelcastTest {


    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Test
    public void testSimpleCluster() {
        HazelcastService hs =  HazelcastServiceTest.createInstance("localhost", "mycluster", "instance1", 5000, new String[] {  "localhost:5000,localhost:5001" });
        HazelcastService hs2 = HazelcastServiceTest.createInstance("localhost", "mycluster", "instance2", 5001, new String[] {  "localhost:5000,localhost:5001" });
        Map aap = hs.getMap("aap");
        hs.getDeclaredClusterSize();

        aap.put("noot", "mies");
        Assert.assertEquals("mies", aap.get("noot"));
        
        Map noot = hs2.getMap("aap");
        System.err.println(">> " + noot.get("noot"));
        Assert.assertEquals("mies", noot.get("noot"));
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Test
    public void testMulticast() throws InterruptedException {
        HazelcastService hs = HazelcastServiceTest.createMulticast("127.0.0.1","mycluster", "instance1", 6000, false);
        HazelcastService hs2 = HazelcastServiceTest.createMulticast("127.0.0.1","mycluster", "instance2", 6001, false);
        Map aap = hs.getMap("aap");
        aap.put("noot", "mies");
        Map noot = hs2.getMap("aap");
        System.err.println(">> " + noot.get("noot"));
//        Assert.assertEquals("mies", noot.get("noot"));
    }

    @After
    public void cleanup() throws Exception {
        Hazelcast.shutdownAll();
    }
}
