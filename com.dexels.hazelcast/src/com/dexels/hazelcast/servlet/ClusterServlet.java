package com.dexels.hazelcast.servlet;

import java.io.IOException;
import java.io.Writer;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import javax.servlet.Servlet;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.Reference;
import org.osgi.service.component.annotations.ReferencePolicy;

import com.dexels.hazelcast.HazelcastService;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.hazelcast.core.Cluster;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.Member;
import com.hazelcast.core.PartitionService;


@Component(name="dexels.cluster.servlet",immediate=true,service={Servlet.class},property={"alias=/cluster","servlet-name=cluster"})
public class ClusterServlet extends HttpServlet {

	private static final long serialVersionUID = -467100129353450660L;
	private HazelcastService hazelcastService;
	final ObjectMapper mapper;
	
	public ClusterServlet() {
	   mapper = new ObjectMapper();
	}
	
	@Reference(unbind="clearHazelcastService",policy=ReferencePolicy.DYNAMIC)
    public void setHazelcastService(HazelcastService hazelcastService) {
        this.hazelcastService = hazelcastService;
    }

	public void clearHazelcastService(HazelcastService hazelcastService) {
        this.hazelcastService = null;
    }
	@Override
	protected void doGet(HttpServletRequest req, HttpServletResponse resp)
			throws ServletException, IOException {
		HazelcastInstance hazelcastInstance = hazelcastService.getHazelcastInstance();
		if(hazelcastInstance==null) {
			resp.sendError(500,"No configured cluster");
			return;
		}
		Cluster cluster = hazelcastInstance.getCluster();
		Set<Member> members = cluster.getMembers();
		Set<Map<String, Object>> printMembers = new HashSet<>();
		
		Member oldestMember =  members.iterator().next();
		Member localMember = cluster.getLocalMember();
		PartitionService partitionService = hazelcastInstance.getPartitionService();
		        
		Map<String,Object> result = new LinkedHashMap<String,Object>();
        String oldestString = String.format("%s @ %s:%s", oldestMember.getUuid(), oldestMember.getSocketAddress().getAddress(),
                oldestMember.getSocketAddress().getPort());	
        String localString = String.format("%s @ %s:%s", localMember.getUuid(), localMember.getSocketAddress().getAddress(),
                localMember.getSocketAddress().getPort());     
		
		result.put("oldest", oldestString);
		result.put("local", localString);
		result.put("isClusterSafe", partitionService.isClusterSafe());
        result.put("isLocalMemberSafe", partitionService.isLocalMemberSafe());
        result.put("clusterSize", members.size());
        
        for (Member m : members) {
            Map<String, Object> memberRes = new LinkedHashMap<>();
            memberRes.put("uuid", m.getUuid());
            memberRes.put("attributes", m.getAttributes());
            memberRes.put("address", m.getSocketAddress());
            memberRes.put("uuid", m.getUuid());
            memberRes.put("isLocalMember", m.localMember());
            printMembers.add(memberRes);
        }
        result.put("members", printMembers);
        if (req.getParameter("debug") != null &&  req.getParameter("debug").equals("true")) {
            result.put("members", members);
        } 
		

		
		resp.setContentType("application/json");
		Writer w = resp.getWriter();
		mapper.writerWithDefaultPrettyPrinter().writeValue(w,result);
		w.close();

	}
	
	
}
