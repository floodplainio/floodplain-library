package com.dexels.kafka.streams.health;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KafkaStreams.State;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.ConfigurationPolicy;
import org.osgi.service.component.annotations.Reference;
import org.osgi.service.component.annotations.ReferenceCardinality;
import org.osgi.service.component.annotations.ReferencePolicy;

import com.dexels.kafka.streams.base.StreamInstance;
import com.dexels.kafka.streams.base.StreamRuntime;
import com.dexels.server.mgmt.api.ServerHealthCheck;

@Component(name="dexels.kafka.streams.health",configurationPolicy=ConfigurationPolicy.REQUIRE)
public class KafkaStreamsHealth implements ServerHealthCheck {

    private StreamRuntime runtime;

	private Map<String,String> issues = new HashMap<>();
    
    @Reference(policy=ReferencePolicy.DYNAMIC,unbind="clearStreamRuntime",cardinality=ReferenceCardinality.OPTIONAL)
    public void setStreamRuntime(StreamRuntime runtime) {
        this.runtime = runtime;
    }
    
    public void clearStreamRuntime(StreamRuntime runtime) {
        this.runtime = null;
    }

	@Override
	public synchronized boolean isOk() {
		issues.clear();
		if(this.runtime==null) {
			issues.put("global", "missing streamsruntime");
		} else {
			Map<String,StreamInstance> str = this.runtime.getStreams();
			for (Entry<String,StreamInstance> e : str.entrySet()) {
				 Collection<KafkaStreams> ss = e.getValue().getStreams();
				 int count = 0;
				 for (KafkaStreams kafkaStream : ss) {
					 String instance = e.getKey();
	   				 State st = kafkaStream.state();
	   				 if(!st.isRunning()) {
	   					 issues.put(instance+"/"+count, "is not running. State: "+st);
	   				 }
	   				 count++;
				}
			}
			
		}
		return issues.isEmpty();
	}

	@Override
	public synchronized String getDescription() {
		return issues.entrySet().stream().map(e->e.getKey()+" - "+e.getValue()).collect(Collectors.joining("\n"));
	}

}
