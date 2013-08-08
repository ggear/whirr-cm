package com.cloudera.whirr.cm.integration;

import org.junit.Assert;
import org.junit.Test;

public abstract class CmServerClusterIT extends BaseITServer {

  @Test
  public void testClusterLifecycle() throws Exception {

    Assert.assertTrue(serverTest.isProvisioned(cluster));
    Assert.assertFalse(serverTest.isConfigured(cluster));
    Assert.assertFalse(serverTest.isStarted(cluster));
    Assert.assertTrue(serverTest.isStopped(cluster));

    Assert.assertTrue(serverTest.configure(cluster));

    Assert.assertTrue(serverTest.isProvisioned(cluster));
    Assert.assertTrue(serverTest.isConfigured(cluster));
    Assert.assertFalse(serverTest.isStarted(cluster));
    Assert.assertTrue(serverTest.isStopped(cluster));

    Assert.assertTrue(serverTest.start(cluster));

    Assert.assertTrue(serverTest.isProvisioned(cluster));
    Assert.assertTrue(serverTest.isConfigured(cluster));
    Assert.assertTrue(serverTest.isStarted(cluster));
    Assert.assertFalse(serverTest.isStopped(cluster));

    Assert.assertTrue(serverTest.stop(cluster));

    Assert.assertTrue(serverTest.isProvisioned(cluster));
    Assert.assertTrue(serverTest.isConfigured(cluster));
    Assert.assertFalse(serverTest.isStarted(cluster));
    Assert.assertTrue(serverTest.isStopped(cluster));

    // TODO
    // Assert.assertTrue(serverTest.provision(cluster));
    // Assert.assertTrue(serverTest.isProvisioned(cluster));
    // Assert.assertTrue(serverTest.unprovision(cluster));
    // Assert.assertFalse(serverTest.isProvisioned(cluster));
    // Assert.assertFalse(serverTest.unprovision(cluster));
    // Assert.assertFalse(serverTest.isProvisioned(cluster));
    // Assert.assertFalse(serverTest.isConfigured(cluster));
    // Assert.assertFalse(serverTest.isStarted(cluster));
    // Assert.assertTrue(serverTest.isStopped(cluster));
    //
    // Assert.assertFalse(serverTest.stop(cluster));
    // Assert.assertFalse(serverTest.unconfigure(cluster));
    // Assert.assertTrue(serverTest.configure(cluster));
    // Assert.assertTrue(serverTest.isProvisioned(cluster));
    // Assert.assertTrue(serverTest.isConfigured(cluster));
    // Assert.assertFalse(serverTest.isStarted(cluster));
    // Assert.assertTrue(serverTest.isStopped(cluster));
    //
    // Assert.assertFalse(serverTest.configure(cluster));
    // Assert.assertTrue(serverTest.isProvisioned(cluster));
    // Assert.assertTrue(serverTest.isConfigured(cluster));
    // Assert.assertFalse(serverTest.isStarted(cluster));
    // Assert.assertTrue(serverTest.isStopped(cluster));
    //
    // Assert.assertTrue(serverTest.unprovision(cluster));
    // Assert.assertFalse(serverTest.isProvisioned(cluster));
    // Assert.assertFalse(serverTest.isConfigured(cluster));
    // Assert.assertFalse(serverTest.isStarted(cluster));
    // Assert.assertTrue(serverTest.isStopped(cluster));
    //
    // Assert.assertTrue(serverTest.start(cluster));
    // Assert.assertTrue(serverTest.isProvisioned(cluster));
    // Assert.assertTrue(serverTest.isConfigured(cluster));
    // Assert.assertTrue(serverTest.isStarted(cluster));
    // Assert.assertFalse(serverTest.isStopped(cluster));
    //
    // Assert.assertFalse(serverTest.start(cluster));
    // Assert.assertTrue(serverTest.isStarted(cluster));
    // Assert.assertFalse(serverTest.isStopped(cluster));
    // Assert.assertFalse(serverTest.start(cluster));
    // Assert.assertTrue(serverTest.isStarted(cluster));
    // Assert.assertFalse(serverTest.isStopped(cluster));
    //
    // Assert.assertTrue(serverTest.stop(cluster));
    // Assert.assertFalse(serverTest.isStarted(cluster));
    // Assert.assertTrue(serverTest.isStopped(cluster));
    // Assert.assertFalse(serverTest.stop(cluster));
    // Assert.assertFalse(serverTest.isStarted(cluster));
    // Assert.assertTrue(serverTest.isStopped(cluster));
    //
    // Assert.assertFalse(serverTest.stop(cluster));
    // Assert.assertFalse(serverTest.isStarted(cluster));
    // Assert.assertTrue(serverTest.isStopped(cluster));
    // Assert.assertFalse(serverTest.stop(cluster));
    // Assert.assertFalse(serverTest.isStarted(cluster));
    // Assert.assertTrue(serverTest.isStopped(cluster));
    //
    // Assert.assertTrue(serverTest.start(cluster));
    // Assert.assertTrue(serverTest.isStarted(cluster));
    // Assert.assertFalse(serverTest.isStopped(cluster));
    //
    // Assert.assertTrue(serverTest.unconfigure(cluster));
    // Assert.assertTrue(serverTest.isProvisioned(cluster));
    // Assert.assertFalse(serverTest.isConfigured(cluster));
    // Assert.assertFalse(serverTest.isStarted(cluster));
    // Assert.assertTrue(serverTest.isStopped(cluster));
    //
    // Assert.assertFalse(serverTest.unconfigure(cluster));
    // Assert.assertTrue(serverTest.isProvisioned(cluster));
    // Assert.assertFalse(serverTest.isConfigured(cluster));
    // Assert.assertFalse(serverTest.isStarted(cluster));
    // Assert.assertTrue(serverTest.isStopped(cluster));

  }

}