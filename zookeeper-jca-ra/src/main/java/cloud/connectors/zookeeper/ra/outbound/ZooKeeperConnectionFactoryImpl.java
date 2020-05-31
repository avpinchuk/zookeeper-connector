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

package cloud.connectors.zookeeper.ra.outbound;

import cloud.connectors.zookeeper.api.ZooKeeperConnection;
import cloud.connectors.zookeeper.api.ZooKeeperConnectionFactory;

import javax.naming.Reference;
import javax.resource.ResourceException;
import javax.resource.spi.ConnectionManager;

/**
 * Is a factory for ZooKeeper connection instances.
 *
 * @author alexa
 */
public class ZooKeeperConnectionFactoryImpl implements ZooKeeperConnectionFactory {

    private static final long serialVersionUID = 1L;

    private final ZooKeeperManagedConnectionFactory managedConnectionFactory;
    private final ConnectionManager connectionManager;
    private Reference reference;

    public ZooKeeperConnectionFactoryImpl(ZooKeeperManagedConnectionFactory managedConnectionFactory,
                                          ConnectionManager connectionManager) {
        this.managedConnectionFactory = managedConnectionFactory;
        this.connectionManager = connectionManager;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ZooKeeperConnection getConnection() throws ResourceException {
        return (ZooKeeperConnection) connectionManager.allocateConnection(managedConnectionFactory, null);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Reference getReference() {
        return reference;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setReference(Reference reference) {
        this.reference = reference;
    }

}
