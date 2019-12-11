package com.dexels.hazelcast;

import java.util.Date;
import java.util.Map;
import java.util.Set;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ILock;
import com.hazelcast.core.Member;

public interface HazelcastService {

    public int getMulticastPort();

    public Date getJoinDate();

    public boolean isAvailable();

    public HazelcastInstance getHazelcastInstance();

    public String getName();

    public int getClusterSize();
    
    public int getDeclaredClusterSize();

    public ILock getLock(String lockObject);

    public Member oldestMember();

    public void deactivate();

    public void manualActivate(Map<String, Object> settings) throws Exception;

    public void activate(String instanceName) throws Exception;

    public boolean isActivated();

    public String getInstanceName();

    public Map<Object, Object> getMap(String identifier);

    public Set<Object> getSet(String name);
}