package com.dexels.hazelcast.test;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.Lock;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.dexels.hazelcast.HazelcastService;
import com.dexels.hazelcast.impl.HazelcastOSGiConfig;
import com.dexels.hazelcast.impl.OSGiHazelcastServiceImpl;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.IAtomicLong;

public class HazelcastServiceTest {
	
	private static HazelcastService hcs1 = null;
	private static HazelcastService hcs2 = null;
	
	@BeforeClass
	public static void setUp() throws Exception {
		hcs1 = createInstance("localhost","mycluster", "test1", 5000, new String[]{"localhost:5000","localhost:5001"});
		hcs2 = createInstance("localhost","mycluster", "test2", 5001, new String[]{"localhost:5000","localhost:5001"});

	}
	
	public static HazelcastService createInstance(String hostName,String clusterName, String instanceName, int port, String[] members) {
		HazelcastOSGiConfig config = new HazelcastOSGiConfig();
		OSGiHazelcastServiceImpl instance = new OSGiHazelcastServiceImpl();
		Map<String,Object> settings = new HashMap<String,Object>();
		
		settings.put("cluster.port",port);
		settings.put("cluster.externalPort",port);
		settings.put("cluster.name",clusterName);
		settings.put("cluster.instance",instanceName);
		settings.put("cluster.members",members);
		settings.put("cluster.hostName",hostName);
		settings.put("cluster.interface","127.0.0.1");
        settings.put("hazelcast.logging.type", "jdk");

		config.activate(settings);
		instance.setHazelcastConfig(config);
		instance.manualActivate((Map<String,Object>)null);
		return instance;
	}
	
    public static  HazelcastService createMulticast(String hostName,String clusterName, String instanceName, int port, boolean async) {
		HazelcastOSGiConfig config = new HazelcastOSGiConfig();
		final OSGiHazelcastServiceImpl instance = new OSGiHazelcastServiceImpl();
		final Map<String,Object> settings = new HashMap<String,Object>();


        System.err.println("create multicast");
        settings.put("cluster.port", port);
        settings.put("cluster.externalPort", port);
        settings.put("cluster.name", clusterName);
        settings.put("cluster.instance", instanceName);
        settings.put("cluster.interface", "127.0.0.1");
		settings.put("cluster.hostName",hostName);
        settings.put("hazelcast.logging.type", "jdk");
      //
        config.activate(settings);
        instance.setHazelcastConfig(config);
        if (async) {
            new Thread() {
                @Override
                public void run() {
                    instance.manualActivate(settings);
                }
            }.start();
        } else {
            instance.activate(settings,null);
        }
        // Wait until the actual instance is set
        while (instance.getHazelcastInstance() == null) {
            try {
                Thread.sleep(250);
            } catch (InterruptedException e) {
            }
        }
        return instance;
    }

	
	@AfterClass
	public static void tearDown() throws Exception {
		hcs1.deactivate();
		hcs2.deactivate();
		Hazelcast.shutdownAll();
	}
	
	@Test
	public void testSetup() throws Exception {
		Assert.assertNotNull(hcs1);
		Assert.assertEquals(true, hcs1.isAvailable());
		Assert.assertNotNull(hcs2);
		Assert.assertEquals(true, hcs2.isAvailable());
	}
	
	@Test
	public void testLock() throws Exception {
		final Lock l = hcs1.getLock("Lock");
		Assert.assertNotNull(l);
		l.lock();
		LockThread t = null;
		// Again.
		try {
		    t = new LockThread(l);
	        t.start();
	        t.join();
	        Assert.assertEquals(false, t.result);
		} finally {
		    l.unlock();
		}

		t = new LockThread(l);
		t.start();
		t.join();
		Assert.assertEquals(true, t.result);
	}
	
	@Test
	public void testLockWithTwoInstances() {
		Lock l1 = hcs1.getLock("Lock");
		l1.lock();
		Lock l2 = hcs2.getLock("Lock");
		boolean result = l2.tryLock();
		Assert.assertEquals(false, result);
		l1.unlock();
		l2 = hcs2.getLock("Lock");
		result = l2.tryLock();
		Assert.assertEquals(true, result);
	}
	
	@SuppressWarnings({"unchecked","rawtypes"})
	@Test 
	public void testMap() {
		Map m1 = hcs1.getMap("MyMap");
		m1.put("Key", "Value");
		Assert.assertNotNull(m1.get("Key"));
		Assert.assertEquals("Value", m1.get("Key"));
		
		Map m2 = hcs2.getMap("MyMap");
		Assert.assertNotNull(m2.get("Key"));
		Assert.assertEquals("Value", m2.get("Key"));
	}
	
	@Test
	public void testDistributedCounters() {
		hcs2.getHazelcastInstance();
		
		long start = System.currentTimeMillis();
		long heapSize = Runtime.getRuntime().freeMemory();
		for ( int i = 0; i < 10000; i++ ) {
		    IAtomicLong an = hcs1.getHazelcastInstance().getAtomicLong("Number"+i);
		  if ( an.get() > 0 ) {
			  System.err.println("Ja!!");
		  }
		}
		long end = System.currentTimeMillis();
		System.err.println("First run: " + ( end - start ) + " millis.");
		for ( int i = 0; i < 10000; i++ ) {
		    IAtomicLong an = hcs1.getHazelcastInstance().getAtomicLong("Number"+i);
			  if ( an.get() > 0 ) {
				  System.err.println("Ja!!");
			  }
			}
		System.gc();
		System.err.println("Second run: " + ( System.currentTimeMillis() - end )  + " millis.");
		start = System.currentTimeMillis();
		for ( int i = 0; i < 10000; i++ ) {
			  IAtomicLong an = hcs2.getHazelcastInstance().getAtomicLong("Number"+i);
			  if ( an.get() > 0 ) {
				  System.err.println("Ja!!");
			  }
			}
		System.err.println("Third run: " + ( System.currentTimeMillis() - start )  + " millis.");
		System.err.println("Total memory: " + ( heapSize - Runtime.getRuntime().freeMemory()  ) / 1024 / 1024 );
	}
	
	@Test
	public void testMyName() {
		Assert.assertEquals("test1", hcs1.getName());
		Assert.assertEquals("test2", hcs2.getName());
	}

	@Test
	public void testClustername() {
		Assert.assertEquals(hcs1.getInstanceName(), "test1");
	}
}
