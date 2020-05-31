/*
 * Copyright (c) 2020 Alexander Pinchuk
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package cloud.connectors.zookeeper.ra.inbound;

import javax.resource.NotSupportedException;
import javax.resource.ResourceException;
import javax.resource.spi.ActivationSpec;
import javax.resource.spi.BootstrapContext;
import javax.resource.spi.Connector;
import javax.resource.spi.ResourceAdapter;
import javax.resource.spi.ResourceAdapterInternalException;
import javax.resource.spi.endpoint.MessageEndpointFactory;
import javax.transaction.xa.XAResource;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * This represents a resource adapter instance and contains operations for
 * lifecycle management and message endpoint setup.This implementation
 * is a JavaBean according to JCA specification.
 *
 * @author alexa
 */
@Connector(
    displayName = "Zookeeper Resource Adapter",
    version = "1.0"
)
@SuppressWarnings({"unused", "RedundantThrows"})
public class ZooKeeperResourceAdapter implements ResourceAdapter {

    private static final Logger logger = Logger.getLogger(ZooKeeperResourceAdapter.class.getName());

    /**
     * A registered watchers.
     */
    private final Map<MessageEndpointFactory, ZooKeeperWatcher> registeredWatchers;

    private BootstrapContext bootstrapContext;

    public ZooKeeperResourceAdapter() {
        this.registeredWatchers = new ConcurrentHashMap<>();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void start(BootstrapContext bootstrapContext) throws ResourceAdapterInternalException {
        this.bootstrapContext = bootstrapContext;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void stop() {
        // go through registered watchers and close them
        for (ZooKeeperWatcher watcher : registeredWatchers.values()) {
            try {
                watcher.close();
            } catch (ResourceException e) {
                logger.log(Level.SEVERE, null, e);
            }
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void endpointActivation(MessageEndpointFactory messageEndpointFactory,
                                   ActivationSpec activationSpec) throws ResourceException {
        if (activationSpec instanceof ZooKeeperActivationSpec) {
            ZooKeeperWatcher watcher = new ZooKeeperWatcher(messageEndpointFactory,
                                                            (ZooKeeperActivationSpec) activationSpec,
                                                            bootstrapContext.getWorkManager());
            registeredWatchers.put(messageEndpointFactory, watcher);
            bootstrapContext.getWorkManager().scheduleWork(watcher);
        } else {
            throw new NotSupportedException("Got endpoint activation for an ActivationSpec of unknown class " +
                                            activationSpec.getClass().getName());
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void endpointDeactivation(MessageEndpointFactory messageEndpointFactory, ActivationSpec activationSpec) {
        ZooKeeperWatcher watcher = registeredWatchers.remove(messageEndpointFactory);
        if (watcher != null) {
            try {
                watcher.close();
            } catch (ResourceException e) {
                logger.log(Level.SEVERE, null, e);
            }
        }
    }

    /**
     * This resource adapter currently does not supports transactions.
     */
    @Override
    public XAResource[] getXAResources(ActivationSpec[] activationSpecs) throws ResourceException {
        return new XAResource[0];
    }

}
