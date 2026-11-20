/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.metrics.jmx;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.remote.JMXConnectorServer;
import javax.management.remote.JMXServiceURL;
import javax.management.remote.rmi.RMIConnectorServer;
import javax.management.remote.rmi.RMIJRMPServerImpl;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.net.MalformedURLException;
import java.rmi.AlreadyBoundException;
import java.rmi.NoSuchObjectException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;

/**
 * JMX Server implementation that JMX clients can connect to.
 *
 * <p>Heavily based on j256 simplejmx project
 *
 * <p>https://github.com/j256/simplejmx/blob/master/src/main/java/com/j256/simplejmx/server/JmxServer.java
 */
class JMXServer {
    private static final Logger LOG = LoggerFactory.getLogger(JMXServer.class);

    private static final String JMX_LOOKUP_NAME = "jmxrmi";

    private Registry rmiRegistry;
    private JMXConnectorServer connector;
    private int port;

    JMXServer() {}

    void start(int port) throws IOException {
        if (rmiRegistry != null && connector != null) {
            LOG.debug("JMXServer is already running.");
            return;
        }
        internalStart(port);
        this.port = port;
    }

    void stop() throws IOException {
        if (connector != null) {
            try {
                connector.stop();
            } finally {
                connector = null;
            }
        }
        if (rmiRegistry != null) {
            try {
                UnicastRemoteObject.unexportObject(rmiRegistry, true);
            } catch (NoSuchObjectException e) {
                throw new IOException("Could not un-export our RMI registry", e);
            } finally {
                rmiRegistry = null;
            }
        }
    }

    int getPort() {
        return port;
    }

    private void internalStart(int port) throws IOException {
        // this allows clients to lookup the JMX service under
        // service:jmx:rmi://<host>:<port>/jndi/rmi://<host>:<port>/jmxrmi
        rmiRegistry = LocateRegistry.createRegistry(port);

        // No JNDI path in the URL: the connector must not bind itself through a remote registry
        // call, we bind its stub below with a local call instead.
        String serviceUrl = "service:jmx:rmi://localhost:" + port;
        JMXServiceURL url;
        try {
            url = new JMXServiceURL(serviceUrl);
        } catch (MalformedURLException e) {
            throw new IllegalArgumentException("Malformed service url created " + serviceUrl, e);
        }

        // shares the registry's port, both are exported with the default socket factories
        final RMIJRMPServerImpl rmiServer = new RMIJRMPServerImpl(port, null, null, null);

        connector =
                new RMIConnectorServer(
                        url, null, rmiServer, ManagementFactory.getPlatformMBeanServer());
        connector.start();

        // the stub only exists once the connector is started; a local bind skips the registry's
        // remote-host access check
        try {
            rmiRegistry.bind(JMX_LOOKUP_NAME, rmiServer.toStub());
        } catch (AlreadyBoundException e) {
            throw new IOException("Could not bind the JMX connector to the RMI registry", e);
        }
    }
}
