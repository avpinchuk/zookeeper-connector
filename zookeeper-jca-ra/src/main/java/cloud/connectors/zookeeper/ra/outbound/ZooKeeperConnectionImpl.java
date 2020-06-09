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
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;

import javax.resource.ResourceException;
import javax.resource.spi.ConnectionManager;
import javax.resource.spi.ConnectionRequestInfo;
import javax.resource.spi.IllegalStateException;
import javax.resource.spi.LazyAssociatableConnectionManager;
import java.util.List;

/**
 * Represents an application-level handle that is used by a client to access the
 * underlying physical connection. The actual physical connection associated with an this
 * instance is represented by a {@code ZooKeeperManagedConnection} class instance.
 *
 * <p>A client gets a connection by using the getConnection method on a connection factory
 * instance.
 *
 * <p>All methods of this class will throw an {@link IllegalStateException} when invoked
 * for closed or inactive connection handle.
 *
 * @author alexa
 * @see cloud.connectors.zookeeper.api.ZooKeeperConnectionFactory ZooKeeperConnectionFactory
 */
public class ZooKeeperConnectionImpl implements ZooKeeperConnection {

    /**
     * The managed connection represents actual physical connection.
     */
    private ZooKeeperManagedConnection managedConnection;
    private final ZooKeeperManagedConnectionFactory connectionFactory;
    private final ConnectionManager connectionManager;
    private final ConnectionRequestInfo requestInfo;

    private boolean closed;

    /**
     * Creates an application level connection handle instance. Must not be used directly.
     *
     * @param managedConnection the associated physical connection
     * @param connectionFactory the managed connection factory
     * @param connectionManager the connection manager
     * @param requestInfo the connection request info
     */
    public ZooKeeperConnectionImpl(ZooKeeperManagedConnection managedConnection,
                                   ZooKeeperManagedConnectionFactory connectionFactory,
                                   ConnectionManager connectionManager,
                                   ConnectionRequestInfo requestInfo) {
        // associates this handle with the managed connection
        this.managedConnection = managedConnection;
        this.connectionFactory = connectionFactory;
        this.connectionManager = connectionManager;
        this.requestInfo = requestInfo;
        this.closed = false;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String create(String path, byte[] data, List<ACL> acl, CreateMode createMode) throws ResourceException {
        checkState();
        if (managedConnection == null) {
            associateConnection();
        }
        return managedConnection.create(path, data, acl, createMode);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String create(String path, byte[] data, List<ACL> acl, CreateMode createMode, Stat stat) throws ResourceException {
        checkState();
        if (managedConnection == null) {
            associateConnection();
        }
        return managedConnection.create(path, data, acl, createMode, stat);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String create(String path, byte[] data, List<ACL> acl, CreateMode createMode, Stat stat, long ttl) throws ResourceException {
        checkState();
        if (managedConnection == null) {
            associateConnection();
        }
        return managedConnection.create(path, data, acl, createMode, stat, ttl);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void delete(String path) throws ResourceException {
        checkState();
        if (managedConnection == null) {
            associateConnection();
        }
        managedConnection.delete(path);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void delete(String path, int version) throws ResourceException {
        checkState();
        if (managedConnection == null) {
            associateConnection();
        }
        managedConnection.delete(path, version);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Stat exists(String path) throws ResourceException {
        checkState();
        if (managedConnection == null) {
            associateConnection();
        }
        return managedConnection.exists(path);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<ACL> getACL(String path) throws ResourceException {
        checkState();
        if (managedConnection == null) {
            associateConnection();
        }
        return managedConnection.getACL(path);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<ACL> getACL(String path, Stat stat) throws ResourceException {
        checkState();
        if (managedConnection == null) {
            associateConnection();
        }
        return managedConnection.getACL(path, stat);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int getAllChildrenNumber(String path) throws ResourceException {
        checkState();
        if (managedConnection == null) {
            associateConnection();
        }
        return managedConnection.getAllChildrenNumber(path);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<String> getChildren(String path) throws ResourceException {
        checkState();
        if (managedConnection == null) {
            associateConnection();
        }
        return managedConnection.getChildren(path);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<String> getChildren(String path, Stat stat) throws ResourceException {
        checkState();
        if (managedConnection == null) {
            associateConnection();
        }
        return managedConnection.getChildren(path, stat);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public byte[] getData(String path) throws ResourceException {
        checkState();
        if (managedConnection == null) {
            associateConnection();
        }
        return managedConnection.getData(path);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public byte[] getData(String path, Stat stat) throws ResourceException {
        checkState();
        if (managedConnection == null) {
            associateConnection();
        }
        return managedConnection.getData(path, stat);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<String> getEphemerals() throws ResourceException {
        checkState();
        if (managedConnection == null) {
            associateConnection();
        }
        return managedConnection.getEphemerals();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<String> getEphemerals(String prefixPath) throws ResourceException {
        checkState();
        if (managedConnection == null) {
            associateConnection();
        }
        return managedConnection.getEphemerals(prefixPath);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Stat setACL(String path, List<ACL> acl) throws ResourceException {
        checkState();
        if (managedConnection == null) {
            associateConnection();
        }
        return managedConnection.setACL(path, acl);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Stat setACL(String path, List<ACL> acl, int version) throws ResourceException {
        checkState();
        if (managedConnection == null) {
            associateConnection();
        }
        return managedConnection.setACL(path, acl, version);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Stat setData(String path, byte[] data) throws ResourceException {
        checkState();
        if (managedConnection == null) {
            associateConnection();
        }
        return managedConnection.setData(path, data);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Stat setData(String path, byte[] data, int version) throws ResourceException {
        checkState();
        if (managedConnection == null) {
            associateConnection();
        }
        return managedConnection.setData(path, data, version);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void close() {
        if (closed) {
            return;
        }

        closed = true;
        if (managedConnection != null) {
            managedConnection.closeHandle(this);
        } else {
            if (connectionManager instanceof LazyAssociatableConnectionManager) {
                LazyAssociatableConnectionManager manager = (LazyAssociatableConnectionManager) connectionManager;
                manager.inactiveConnectionClosed(this, connectionFactory);
            }
        }
    }

    /**
     * Returns the associated physical connection.
     *
     * @return the associated physical connection or {@code null} if inactive or closed
     */
    ZooKeeperManagedConnection getManagedConnection() {
        return managedConnection;
    }

    /**
     * Associates and activates this handle with the physical connection if
     * {@code managedConnection} is not {@code null}, disassociates and deactivates
     * otherwise.
     *
     * @param managedConnection the underlying physical connection
     */
    void setManagedConnection(ZooKeeperManagedConnection managedConnection) {
        // associates the given managed connection with this handle
        this.managedConnection = managedConnection;
    }

    /**
     * Checks the state of this connection handle. Client can use this handle
     * only when its associated with a managed connection.
     *
     * @throws IllegalStateException if this handle is inactive or closed
     */
    private void checkState() throws IllegalStateException {
        if (closed) {
            throw new IllegalStateException("Cannot perform operation on a closed connection");
        }
    }

    private void associateConnection() throws ResourceException {
        if (connectionManager instanceof LazyAssociatableConnectionManager) {
            LazyAssociatableConnectionManager manager = (LazyAssociatableConnectionManager) connectionManager;
            manager.associateConnection(this, connectionFactory, requestInfo);
        }
    }

}
