package com.dexels.hazelcast.kubernetes.impl;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.osgi.service.component.annotations.Activate;
import org.osgi.service.component.annotations.Component;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dexels.hazelcast.kubernetes.UpdateClusterMembers;
import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

import io.kubernetes.client.ApiClient;
import io.kubernetes.client.ApiException;
import io.kubernetes.client.Configuration;
import io.kubernetes.client.apis.AppsV1Api;
import io.kubernetes.client.apis.CoreV1Api;
import io.kubernetes.client.apis.ExtensionsV1beta1Api;
import io.kubernetes.client.custom.V1Patch;
import io.kubernetes.client.models.ExtensionsV1beta1Deployment;
import io.kubernetes.client.models.V1Container;
import io.kubernetes.client.models.V1Deployment;
import io.kubernetes.client.models.V1DeploymentList;
import io.kubernetes.client.models.V1ExecAction;
import io.kubernetes.client.util.Config;

@Component(immediate = true)
public class UpdateClusterMembersImpl implements UpdateClusterMembers {

    private CoreV1Api api;
    private ExtensionsV1beta1Api extensionsApi;
    private AppsV1Api appsApi;
    private String namespace;
    private final static int TERMINATION_GRACE_PERIOD_SECONDS_MARGIN = 10;
    private final static Logger logger = LoggerFactory.getLogger(UpdateClusterMembersImpl.class);

    @Activate
    public void activate() throws IOException {
        ApiClient client = createAppClient();
        Configuration.setDefaultApiClient(client);
        api = new CoreV1Api();
        extensionsApi = new ExtensionsV1beta1Api();
        appsApi = new AppsV1Api();
    }

    private ApiClient createAppClient() throws IOException {
        String token;
        try {
            token = fileContent("/var/run/secrets/kubernetes.io/serviceaccount/token");
            this.namespace = fileContent("/var/run/secrets/kubernetes.io/serviceaccount/namespace");
            String cert = fileContent("/var/run/secrets/kubernetes.io/serviceaccount/ca.crt");
        } catch (IOException e) {
            logger.info("Detected that I'm not running inside kube, using fallback.");
            return createFallbackAppClient();
        }

        String host = System.getenv("KUBERNETES_SERVICE_HOST");
        String port = System.getenv("KUBERNETES_SERVICE_PORT");
        if (host == null || port == null) {
            logger.info(
                    "Detected that I'm not running inside kube: no KUBERNETES_SERVICE_HOST or KUBERNETES_SERVICE_PORT detected, using fallback.");
            return createFallbackAppClient();
        }
        String url = "https://" + host + ":" + port;
        ApiClient client = Config.fromToken(url, token, false);
        return client;
    }

    private ApiClient createFallbackAppClient() throws IOException {
        this.namespace = null;

        String confingEnv = System.getenv("KUBECONFIG");
        if (confingEnv == null || confingEnv.isEmpty()) {
            return Config.defaultClient();
        } else {
            return Config.fromConfig(confingEnv);
        }

    }

    public String fileContent(String path) throws IOException {
        List<String> l = Files.readAllLines(Paths.get(path));
        return l.stream().collect(Collectors.joining(""));
    }

    private ArrayList<JsonObject> createLifecyclePatchJsonObjectsForContainers(List<V1Container> containers, int timeout) {
        String jsonPatchCommandStr;
        int cont = 0;
        String commandValue;
        ArrayList<JsonObject> arr = new ArrayList<>();
        for (V1Container container : containers) {
            if (container.getLifecycle() != null && container.getLifecycle().getPreStop() != null
                    && container.getLifecycle().getPreStop().getExec() != null) {
                V1ExecAction execAction = container.getLifecycle().getPreStop().getExec();
                int com = 0;
                for (String command : execAction.getCommand()) {
                    if (command.contains("timeout")) {
                        String existingTimeout = command.split("timeout=")[1];
                        if (existingTimeout.equals(timeout)) {
                            commandValue = command.replaceAll("timeout=[0-9]*",
                                    "timeout=" + String.valueOf(Integer.parseInt(existingTimeout) + 1));
                        } else {
                            commandValue = command.replaceAll("timeout=[0-9]*", "timeout=" + timeout);
                        }
                        commandValue = commandValue.replace("\"", "\\\"");
                        jsonPatchCommandStr = "{ \"op\" : \"replace\", \"path\" : \"/spec/template/spec/containers/" + String.valueOf(cont)
                                + "/lifecycle/preStop/exec/command/" + String.valueOf(com) + "\",\"value\":\"" + commandValue + "\"}";
                        arr.add(((JsonElement) deserialize(jsonPatchCommandStr, JsonElement.class)).getAsJsonObject());
                    }
                    com += 1;
                }
            }
            cont += 1;
        }
        return arr;
    }

    public String createTerminationGracePeriodPatchJsonStr(int timeout, Long currenttgps) {
        int newtgps = 0;
        if (timeout + TERMINATION_GRACE_PERIOD_SECONDS_MARGIN == currenttgps) {
            newtgps = timeout + TERMINATION_GRACE_PERIOD_SECONDS_MARGIN + 1;
        } else {
            newtgps = timeout + TERMINATION_GRACE_PERIOD_SECONDS_MARGIN;
        }
        return "{\"op\":\"replace\",\"path\":\"/spec/template/spec/terminationGracePeriodSeconds\",\"value\":" + String.valueOf(newtgps)
                + "}";
    }
//
//
//    @Override
//    public void rollingRestartPods(String[] pods, String namespace, String cluster, int timeout) {
//        // TODO: Implement
//    }
//    
//    @Override
//    public void rollingRestartDeploymentWithPods(String[] pods, String namespace, String cluster, int timeout) {
//        // TODO: Implement
//    }

//    @Override
//    public void rollingRestartDeployment(String namespace, String deployment, int timeout) {
//        try {
//            ExtensionsV1beta1Deployment mdp = extensionsApi.readNamespacedDeployment(deployment, namespace, "", true, false);
//            ArrayList<JsonObject> arr = createLifecyclePatchJsonObjectsForContainers(mdp.getSpec().getTemplate().getSpec().getContainers(),
//                    timeout);
//            arr.add(((JsonElement) deserialize(createTerminationGracePeriodPatchJsonStr(timeout,
//                    mdp.getSpec().getTemplate().getSpec().getTerminationGracePeriodSeconds()), JsonElement.class)).getAsJsonObject());
//            extensionsApi.patchNamespacedDeployment(deployment, namespace, arr, "","");
//        } catch (ApiException e1) {
//            logger.error("Something went wrong restarting deployment " + deployment + " : " + e1);
//        }
//    }
    
    @Override
    public void rollingRestartDeploymentByLabelSelector(String namespace, String label, int timeout) {
        logger.debug("Restarting deployment by label :" + label);
        try {
        	V1DeploymentList deployments =  appsApi.listNamespacedDeployment(namespace, "", "", "", "", 10, null, 10, false);

//        	V1DeploymentList deployments = appsApi.listNamespacedDeployment(namespace, true,"", "", "", false, label, 1, "", 10,false);
            for (V1Deployment deployment : deployments.getItems()) {
                ArrayList<JsonObject> arr = createLifecyclePatchJsonObjectsForContainers(deployment.getSpec().getTemplate().getSpec().getContainers(),
                        timeout);
                arr.add(((JsonElement) deserialize(createTerminationGracePeriodPatchJsonStr(timeout,
                        deployment.getSpec().getTemplate().getSpec().getTerminationGracePeriodSeconds()), JsonElement.class)).getAsJsonObject());
                String rollingRestartString = arr.stream().map(e->e.toString()).collect(Collectors.joining("\n"));
                System.err.println("Rolling String: "+rollingRestartString);
                V1Patch pp = new V1Patch(rollingRestartString);
                appsApi.patchNamespacedDeployment(deployment.getMetadata().getName(), namespace, pp, "","","",false);
            }
        } catch (ApiException e) {
            logger.error("Something went wrong restarting deployments with label " + label + " : " + e);
        }
    }

    public Object deserialize(String jsonStr, Class<?> targetClass) {
        Object obj = (new Gson()).fromJson(jsonStr, targetClass);
        return obj;
    }

}
