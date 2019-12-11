package com.dexels.hazelcast.kubernetes;

public interface UpdateClusterMembers {
//    public void rollingRestartPods(String[] pods, String namespace, String cluster, int timeout);
//    public void rollingRestartDeploymentWithPods(String[] pods, String namespace, String cluster, int timeout);
//    public void rollingRestartDeployment(String namespace, String cluster, int timeout);
//    public Object deserialize(String jsonStr, Class<?> targetClass);

    public void rollingRestartDeploymentByLabelSelector(String namespace, String cluster, int timeout);
}
