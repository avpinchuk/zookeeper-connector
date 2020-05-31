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
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;

import javax.resource.NotSupportedException;
import javax.resource.ResourceException;
import javax.resource.spi.ConnectionEvent;
import javax.resource.spi.ConnectionEventListener;
import javax.resource.spi.ConnectionRequestInfo;
import javax.resource.spi.LocalTransaction;
import javax.resource.spi.ManagedConnection;
import javax.resource.spi.ManagedConnectionMetaData;
import javax.security.auth.Subject;
import javax.transaction.xa.XAResource;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Represents a physical connection to the underlying ZooKeeper server.
 *
 * <p><strong>This adapter currently does not supports transactions.</strong>
 *
 * @author alexa
 */
@SuppressWarnings("RedundantThrows")
public class ZooKeeperManagedConnection implements ManagedConnection, ZooKeeperConnection {

    private static final Logger logger = Logger.getLogger(ZooKeeperManagedConnection.class.getName());

    @SuppressWarnings({"FieldCanBeLocal", "unused"})
    private final Subject subject;
    @SuppressWarnings({"FieldCanBeLocal", "unused"})
    private final ConnectionRequestInfo connectionRequestInfo;
    private final ZooKeeper zookeeper;
    private PrintWriter logWriter;
    private ZooKeeperConnectionImpl connectionHandle;
    // We doesn't work with this set directly.
    // They managed by the application server.
    private final Set<ConnectionEventListener> eventListeners;

    public ZooKeeperManagedConnection(ZooKeeperManagedConnectionFactory managedConnectionFactory,
                                      Subject subject,
                                      ConnectionRequestInfo connectionRequestInfo) throws ResourceException {
        this.subject = subject;
        this.connectionRequestInfo = connectionRequestInfo;
        this.eventListeners = new HashSet<>();

        try {
            this.zookeeper = new ZooKeeper(managedConnectionFactory.getConnectString(),
                                           managedConnectionFactory.getSessionTimeout(),
                                           null,
                                           managedConnectionFactory.isCanBeReadOnly());
        } catch (IOException e) {
            throw new ResourceException("Unable to create the ZooKeeper client connection", e);
        }
    }

    /**
     * {@inheritDoc}
     *
     * <p><strong>The {@code subject} and {@code connectRequestInfo} currently
     * does not used.</strong>
     */
    @Override
    public Object getConnection(Subject subject, ConnectionRequestInfo connectionRequestInfo) throws ResourceException {
        // disassociates the current application level
        // connection handle from this managed connection
        if (connectionHandle != null) {
            connectionHandle.setManagedConnection(null);
        }
        // create new application level connection handle and
        // associate it with this managed connection
        connectionHandle = new ZooKeeperConnectionImpl(this);
        return connectionHandle;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void destroy() throws ResourceException {
        try {
            zookeeper.close();
        } catch (InterruptedException e) {
            throw new ResourceException("Unable to close the ZooKeeper client connection", e);
        }
    }

    /**
     * {@inheritDoc}
     *
     * <p>Invokes during close application level handle phase.
     */
    @Override
    public void cleanup() throws ResourceException {
        disassociateConnection();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void associateConnection(Object connection) throws ResourceException {
        if (connection instanceof ZooKeeperConnectionImpl) {
            // disassociates this managed connection from current
            // application level connection handle
            connectionHandle.setManagedConnection(null);
            ZooKeeperConnectionImpl handle = (ZooKeeperConnectionImpl) connection;
            ZooKeeperManagedConnection managedConnection = handle.getManagedConnection();
            if (managedConnection != null) {
                // disassociates the new handle's
                // managed connection from its handle
                managedConnection.disassociateConnection();
            }
            // associates the new handle with this
            // managed connection
            handle.setManagedConnection(this);
            // associates this managed connection with new handle
            connectionHandle = handle;
        }
    }

    /**
     * Disassociates the current application level connection handle from
     * this managed connection.
     */
    void disassociateConnection() {
        connectionHandle = null;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void addConnectionEventListener(ConnectionEventListener connectionEventListener) {
        eventListeners.add(connectionEventListener);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void removeConnectionEventListener(ConnectionEventListener connectionEventListener) {
        eventListeners.remove(connectionEventListener);
    }

    /**
     * {@inheritDoc}
     *
     * <p><strong>Transactions does not supported yet.</strong>
     */
    @Override
    public XAResource getXAResource() throws ResourceException {
        throw new NotSupportedException("Not supported yet");
    }

    /**
     * {@inheritDoc}
     *
     * <p><strong>Transactions does not supported yet.</strong>
     */
    @Override
    public LocalTransaction getLocalTransaction() throws ResourceException {
        throw new NotSupportedException("Not supported yet");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ManagedConnectionMetaData getMetaData() throws ResourceException {
        return new ZooKeeperManagedConnectionMetadata();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setLogWriter(PrintWriter logWriter) throws ResourceException {
        this.logWriter = logWriter;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public PrintWriter getLogWriter() throws ResourceException {
        return logWriter;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String create(String path, byte[] data, List<ACL> acl, CreateMode createMode) throws ResourceException {
        try {
            return zookeeper.create(path, data, acl, createMode);
        } catch (KeeperException | InterruptedException e) {
            throw new ResourceException("Cannot create node " + path, e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String create(String path, byte[] data, List<ACL> acl, CreateMode createMode, Stat stat) throws ResourceException {
        try {
            return zookeeper.create(path, data, acl, createMode, stat);
        } catch (KeeperException | InterruptedException e) {
            throw new ResourceException("Cannot create node " + path, e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String create(String path, byte[] data, List<ACL> acl, CreateMode createMode, Stat stat, long ttl) throws ResourceException {
        try {
            return zookeeper.create(path, data, acl, createMode, stat, ttl);
        } catch (KeeperException | InterruptedException e) {
            throw new ResourceException("Cannot create node " + path, e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void delete(String path) throws ResourceException {
        try {
            zookeeper.delete(path, -1);
        } catch (KeeperException | InterruptedException e) {
            throw new ResourceException("Cannot delete node " + path, e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void delete(String path, int version) throws ResourceException {
        try {
            zookeeper.delete(path, version);
        } catch (KeeperException | InterruptedException e) {
            throw new ResourceException("Cannot delete node " + path, e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Stat exists(String path) throws ResourceException {
        try {
            return zookeeper.exists(path, false);
        } catch (KeeperException | InterruptedException e) {
            throw new ResourceException("Cannot check if node " + path + " exists", e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<ACL> getACL(String path) throws ResourceException {
        try {
            return zookeeper.getACL(path, null);
        } catch (KeeperException | InterruptedException e) {
            throw new ResourceException("Cannot get ACL for node " + path, e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<ACL> getACL(String path, Stat stat) throws ResourceException {
        try {
            return zookeeper.getACL(path, stat);
        } catch (KeeperException | InterruptedException e) {
            throw new ResourceException("Cannot get ACL for node " + path, e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int getAllChildrenNumber(String path) throws ResourceException {
        try {
            return zookeeper.getAllChildrenNumber(path);
        } catch (KeeperException | InterruptedException e) {
            throw new ResourceException("Cannot get children number for node " + path, e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<String> getChildren(String path) throws ResourceException {
        try {
            return zookeeper.getChildren(path, false);
        } catch (KeeperException | InterruptedException e) {
            throw new ResourceException("Cannot get children for node " + path, e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<String> getChildren(String path, Stat stat) throws ResourceException {
        try {
            return zookeeper.getChildren(path, false, stat);
        } catch (KeeperException | InterruptedException e) {
            throw new ResourceException("Cannot get children for node " + path, e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public byte[] getData(String path) throws ResourceException {
        try {
            return zookeeper.getData(path, false, null);
        } catch (KeeperException | InterruptedException e) {
            throw new ResourceException("Cannot get data for node " + path, e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public byte[] getData(String path, Stat stat) throws ResourceException {
        try {
            return zookeeper.getData(path, false, stat);
        } catch (KeeperException | InterruptedException e) {
            throw new ResourceException("Cannot get data for node " + path, e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<String> getEphemerals() throws ResourceException {
        try {
            return zookeeper.getEphemerals();
        } catch (KeeperException | InterruptedException e) {
            throw new ResourceException("Cannot get ephemeral nodes for session", e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<String> getEphemerals(String prefixPath) throws ResourceException {
        try {
            return zookeeper.getEphemerals(prefixPath);
        } catch (KeeperException | InterruptedException e) {
            throw new ResourceException("Cannot get ephemeral nodes with prefix " +
                                        prefixPath + " for session", e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Stat setACL(String path, List<ACL> acl) throws ResourceException {
        try {
            return zookeeper.setACL(path, acl, -1);
        } catch (KeeperException | InterruptedException e) {
            throw new ResourceException("Cannot set ACL for node " + path, e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Stat setACL(String path, List<ACL> acl, int version) throws ResourceException {
        try {
            return zookeeper.setACL(path, acl, version);
        } catch (KeeperException | InterruptedException e) {
            throw new ResourceException("Cannot set ACL for node " + path, e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Stat setData(String path, byte[] data) throws ResourceException {
        try {
            return zookeeper.setData(path, data, -1);
        } catch (KeeperException | InterruptedException e) {
            throw new ResourceException("Cannot set data for node " + path, e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Stat setData(String path, byte[] data, int version) throws ResourceException {
        try {
            return zookeeper.setData(path, data, version);
        } catch (KeeperException | InterruptedException e) {
            throw new ResourceException("Cannot set data for node " + path, e);
        }
    }

    /**
     * This method should be never called. Implemented only for consistency.
     */
    @Override
    public void close() {
        try {
            destroy();
        } catch (ResourceException e) {
            logger.log(Level.SEVERE, null, e);
        }
    }

    /**
     * Notifies all {@code ConnectionEventListener}s for handle close event.
     * Application server used this for put connection handle back to connection
     * pool.
     *
     * @param handle the application level connection handle
     */
    void closeHandle(ZooKeeperConnection handle) {
        ConnectionEvent event = new ConnectionEvent(this, ConnectionEvent.CONNECTION_CLOSED);
        event.setConnectionHandle(handle);
        eventListeners.forEach(eventListener -> eventListener.connectionClosed(event));
    }

}
