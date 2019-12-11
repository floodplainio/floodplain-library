package com.dexels.keystoreprovider.impl;

import java.io.File;
import java.util.Hashtable;

import org.apache.felix.fileinstall.ArtifactInstaller;
import org.osgi.framework.BundleContext;
import org.osgi.framework.ServiceRegistration;
import org.osgi.service.component.annotations.Activate;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.Deactivate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dexels.keystoreprovider.api.KeyStoreProvider;

@Component(name = "dexels.keystore.artifact")
public class KeyStoreArtifactInstaller implements ArtifactInstaller, KeyStoreProvider {

    private volatile File myArtifact = null;

    @SuppressWarnings("rawtypes")
    private ServiceRegistration registration = null;
    private BundleContext bundleContext;

    private static final Logger logger = LoggerFactory.getLogger(KeyStoreArtifactInstaller.class);

    @Activate
    public void activate(BundleContext bundleContext) {
        this.bundleContext = bundleContext;
    }

    @Deactivate
    public void deactivate() {
        if (registration != null) {
            registration.unregister();
        }
        bundleContext = null;
        registration = null;
    }

    @Override
    public boolean canHandle(File f) {
        return f.getName().toLowerCase().endsWith(".jks");
    }

    @Override
    public void install(File f) throws Exception {
        if (myArtifact != null) {
            logger.warn("Keystore Artifact already set! Multiple keystores are not supported, and will cause unexpected behaviour");
        }
        myArtifact = f;
        registration = bundleContext.registerService(KeyStoreProvider.class, this, new Hashtable<String, Object>());

    }

    @Override
    public void uninstall(File f) throws Exception {
        myArtifact = null;
        if (registration != null) {
            registration.unregister();
        }

    }

    @Override
    public void update(File f) throws Exception {
        // no op
    }

    @Override
    public File getKeyStorePath() {
        return myArtifact;
    }

}
