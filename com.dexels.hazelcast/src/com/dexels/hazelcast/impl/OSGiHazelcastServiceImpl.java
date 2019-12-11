package com.dexels.hazelcast.impl;

import java.io.Serializable;
import java.net.InetSocketAddress;
import java.util.Date;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import org.osgi.service.component.ComponentContext;
import org.osgi.service.component.annotations.Activate;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.Deactivate;
import org.osgi.service.component.annotations.Reference;
import org.osgi.service.component.annotations.ReferencePolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dexels.hazelcast.HazelcastService;
import com.dexels.hazelcast.HazelcastServiceMXBean;
import com.hazelcast.config.Config;
import com.hazelcast.core.DistributedObject;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ILock;
import com.hazelcast.core.LifecycleEvent;
import com.hazelcast.core.LifecycleListener;
import com.hazelcast.core.Member;
import com.hazelcast.core.MemberAttributeEvent;
import com.hazelcast.core.MembershipEvent;
import com.hazelcast.core.MembershipListener;

@Component(name="dexels.hazelcast.service",immediate=true,service={HazelcastService.class,HazelcastServiceMXBean.class})
public class OSGiHazelcastServiceImpl implements HazelcastService, HazelcastServiceMXBean,MembershipListener,  Serializable, LifecycleListener {

	private static final long serialVersionUID = -6317814975509547052L;
	private transient HazelcastInstance instance;
	private Date joinDate;
	private boolean available = false;
	
	private transient HazelcastOSGiConfig config = null;
	private transient ClassLoader classLoader;
	
	private static final Logger logger = LoggerFactory.getLogger(OSGiHazelcastServiceImpl.class);
	private AtomicReference<ComponentContext> componentContextReference = new AtomicReference<>();

	@Override
	public void manualActivate(Map<String,Object> settings) {
		this.activate(settings, (ComponentContext)null);
	}
	
	
	
	@Activate
	public void activate(Map<String,Object> settings, ComponentContext context) {
	    logger.info("Activating HazelcastService");
		try {
			if(classLoader!=null) {
				config.setClassLoader(classLoader);
			}
			instance = Hazelcast.newHazelcastInstance(config);
			instance.getCluster().addMembershipListener(this);
			instance.getLifecycleService().addLifecycleListener(this);
			componentContextReference.set(context);

			joinDate = new Date();
			available = true;
		} catch (Exception e) {
			logger.error("Hazelcast config error: ", e);
		}
	}
	
	@Override
    @Deactivate
    public void deactivate() {
        logger.info("Deactivating HazelcastService");
        if(instance!=null && instance.getLifecycleService() != null && instance.getLifecycleService().isRunning()) {
            // Check if we are safe
            if (!instance.getPartitionService().isLocalMemberSafe()) {
                logger.warn("SHUTTING DOWN HAZELCAST INSTANCE IN UNSAFE STATE!");
            }
            this.componentContextReference.set(null);
            instance.getLifecycleService().shutdown();
        } else {
        	logger.info("Not shutting down hazelcast, the cluster seems down already.");
        }
        logger.info("Deactivated HazelcastService");
        joinDate = null;
        available = false;
    }
	
	@Reference(policy=ReferencePolicy.DYNAMIC,unbind="clearClassLoader",target="(tag=hazelcast)")
	public void setClassLoader(ClassLoader classLoader) {
		this.classLoader = classLoader;
	}
	
	public void clearClassLoader(ClassLoader cls) {
		this.classLoader = null;
	}
	@Reference(policy=ReferencePolicy.DYNAMIC,unbind="clearHazelcastConfig")
	public void setHazelcastConfig(HazelcastOSGiConfig config) {
		this.config = config;
	}
	
	public void clearHazelcastConfig(Config conf) {
		this.config = null;
	}

	
	@Override
	public String getClusterOverview() {
		StringBuilder sb = new StringBuilder();
		Iterator<Member> members = getHazelcastInstance().getCluster().getMembers().iterator();
		while ( members.hasNext() ) {
			Member member = members.next();
	
			InetSocketAddress address = member.getSocketAddress();
			sb.append(address + ",");
		}
		String result = sb.toString();
		return result.substring(0, result.length()-1);
	}
	
	// TODO (mxbean) this uses *all* instances. Doesn't make sense, as every instance will be its own mxbean 
	@Override
	public String getRegisteredCollections() {
		StringBuilder sb = new StringBuilder();
		Iterator<DistributedObject> collections = getHazelcastInstance().getDistributedObjects().iterator();
		        
		while ( collections.hasNext() ) {
		    DistributedObject object = collections.next();
			sb.append("(name=" + object.getName() + ",type="+object.getClass().getName()+"),");
		}
		String result = sb.toString();
		return result.substring(0, result.length()-1);
	}

	@Override
	public int getMulticastPort() {
		return config.getNetworkConfig().getPort() ;
	}

	@Override
	public Date getJoinDate() {
		return joinDate;
	}

	@Override
	public boolean isAvailable() {
		return available;
	}

	@Override
	public HazelcastInstance getHazelcastInstance() {
		return instance;
	}

	@Override
	public String getName() {
		return config.getInstanceName();
	}

	@Override
	public int getClusterSize() {
		return instance.getCluster().getMembers().size();
	}
	
	
	@Override
	public int getDeclaredClusterSize() {
	   return config.getClusterSize() + 1;
	}


	@Override
	public ILock getLock(String lockObject) {
		return instance.getLock(lockObject);
	}

    @Override
	public Map<Object, Object> getMap(String identifier) {
		return instance.getMap(identifier);
	}

	@Override
	public Member oldestMember() {
		return getHazelcastInstance().getCluster().getMembers().iterator().next();
	}


	@Override
	public void activate(String instanceName) throws Exception {
		logger.error("Not supported in OSGi");
	}

	@Override
	public Set<Object> getSet(String name) {
		return instance.getSet(name); 
	}

	@Override
	public boolean isActivated() {
		return isAvailable();
	}

	@Override
	public String getInstanceName() {
		return config.getInstanceName();
	}

	@Override
	public void memberAdded(MembershipEvent arg0) {
		logger.info("Member {} was added to Navajo/Hazelcast cluster",arg0.getMember().getSocketAddress());
	}

	@Override
	public void memberRemoved(MembershipEvent arg0) {
		logger.info("Member {} was removed from Navajo/Hazelcast cluster",arg0.getMember().getSocketAddress());
	}

    @Override
    public void memberAttributeChanged(MemberAttributeEvent arg0) {
       // Not that interesting...
        
    }

	@Override
	public void stateChanged(LifecycleEvent e) {
		LifecycleEvent.LifecycleState state = e.getState();
		switch (state) {
		case SHUTDOWN:
		case SHUTTING_DOWN:
		case MERGE_FAILED:
			ComponentContext ref = componentContextReference.get();
			if(ref!=null) {
				ref.getComponentInstance().dispose();
			} else {
				logger.warn("No component context detected!");
			}
			//
			break;
		case CLIENT_CONNECTED:
		case CLIENT_DISCONNECTED:
		case MERGED:
		case MERGING:
		case STARTED:
		case STARTING:
			break;
		}
		
	}
}
