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
import org.apache.zookeeper.server.ZooKeeperServer;

import javax.resource.NotSupportedException;
import javax.resource.ResourceException;
import javax.resource.spi.ConfigProperty;
import javax.resource.spi.ConnectionDefinition;
import javax.resource.spi.ConnectionManager;
import javax.resource.spi.ConnectionRequestInfo;
import javax.resource.spi.ManagedConnection;
import javax.resource.spi.ManagedConnectionFactory;
import javax.resource.spi.ResourceAdapter;
import javax.resource.spi.ResourceAdapterAssociation;
import javax.security.auth.Subject;
import java.io.PrintWriter;
import java.util.Iterator;
import java.util.Objects;
import java.util.Set;

/**
 * This class instance is a factory of both ZooKeeper managed connection
 * and ZooKeeper connection factory instances. This class supports connection
 * pooling by providing methods for matching and creation of managed connection
 * instance. A managed connection factory instance is required to be a JavaBean.
 *
 * @author alexa
 * @see ZooKeeperManagedConnection
 * @see ZooKeeperConnectionFactory
 */
@ConnectionDefinition(
    connection = ZooKeeperConnection.class,
    connectionImpl = ZooKeeperConnectionImpl.class,
    connectionFactory = ZooKeeperConnectionFactory.class,
    connectionFactoryImpl = ZooKeeperConnectionFactoryImpl.class
)
@SuppressWarnings({"unused", "RedundantThrows"})
public class ZooKeeperManagedConnectionFactory implements ManagedConnectionFactory, ResourceAdapterAssociation {

    private static final long serialVersionUID = 1L;

    /**
     * A comma separated host:port pairs, each corresponds to a ZooKeeper server,
     * e.g. "127.0.0.1:3000,127.0.0.1:3001,127.0.0.1:3002". If the optional chroot
     * suffix is used, the example would look like
     * "127.0.0.1:3000,127.0.0.1:3001,127.0.0.1:3002/app/a", where the client would
     * be rooted at "/app/a" and all paths would be relative to this root, i.e.
     * getting/setting/etc... "/foo/bar" would result in operations being run on
     * "/app/a/foo/bar" (from the server perspective).
     */
    @ConfigProperty(type = String.class, defaultValue = "localhost:2181")
    private String connectString = "localhost:2181";

    /**
     * Session timeout in milliseconds.
     */
    @ConfigProperty(type = Integer.class)
    private Integer sessionTimeout = ZooKeeperServer.DEFAULT_TICK_TIME * 2;

    /**
     * Whether the created client is allowed to go to read-only mode in case of partitioning.
     * Read-only mode basically means that if the client can't find any majority servers
     * but there's partitioned server it could reach, it connects to one in read-only mode,
     * i.e. read requests are allowed while write requests are not. It continues seeking
     * for majority in the background.
     */
    @ConfigProperty(type = Boolean.class, defaultValue = "false")
    private Boolean canBeReadOnly = Boolean.FALSE;

    /**
     * The log writer.
     */
    private PrintWriter logWriter;

    /**
     * The resource adapter
     */
    private ResourceAdapter resourceAdapter;

    private ConnectionManager connectionManager;

    /**
     * Get the {@code connectString}.
     *
     * @return the connect string
     */
    public String getConnectString() {
        return connectString;
    }

    /**
     * Set the {@code connectString}.
     *
     * @param connectString the connect string
     */
    public void setConnectString(String connectString) {
        this.connectString = connectString;
    }

    /**
     * Get the {@code sessionTimeout}.
     *
     * @return the session timeout
     */
    public Integer getSessionTimeout() {
        return sessionTimeout;
    }

    /**
     * Set the {@code sessionTimeout}.
     *
     * @param sessionTimeout the session timeout
     */
    public void setSessionTimeout(Integer sessionTimeout) {
        this.sessionTimeout = sessionTimeout;
    }

    /**
     * Get the {@code canBeReadOnly}.
     *
     * @return the read-only flag
     */
    public Boolean isCanBeReadOnly() {
        return canBeReadOnly;
    }

    /**
     * Set the {@code canBeReadOnly}.
     *
     * @param canBeReadOnly the read-only flag
     */
    public void setCanBeReadOnly(Boolean canBeReadOnly) {
        this.canBeReadOnly = canBeReadOnly;
    }

    /**
     * {@inheritDoc}
     *
     * @return the {@link ZooKeeperConnectionFactoryImpl ZooKeeperConnectionFactory} instance
     */
    @Override
    public Object createConnectionFactory(ConnectionManager connectionManager) throws ResourceException {
        this.connectionManager = connectionManager;
        return new ZooKeeperConnectionFactoryImpl(this, connectionManager);
    }

    /**
     * This adapter currently does not supports a non-managed environments.
     * @return nothing
     * @throws ResourceException throws {@link NotSupportedException} subclass
     */
    @Override
    public Object createConnectionFactory() throws ResourceException {
        throw new NotSupportedException("This resource adapter doesn't support non-managed environment");
    }

    /**
     * {@inheritDoc}
     *
     * @param subject not used
     * @param connectionRequestInfo not used
     * @return the {@link ZooKeeperManagedConnection}
     */
    @Override
    public ManagedConnection createManagedConnection(Subject subject, ConnectionRequestInfo connectionRequestInfo) throws ResourceException {
        return new ZooKeeperManagedConnection(this, connectionManager, subject, connectionRequestInfo);
    }

    /**
     * {@inheritDoc}
     *
     * @param set the candidate set
     * @param subject not used
     * @param connectionRequestInfo not used
     * @return the {@link ZooKeeperManagedConnection}
     */
    @Override
    @SuppressWarnings("rawtypes")
    public ManagedConnection matchManagedConnections(Set set, Subject subject, ConnectionRequestInfo connectionRequestInfo) throws ResourceException {
        ManagedConnection connection = null;
        Iterator iterator = set.iterator();
        while (connection == null && iterator.hasNext()) {
            ManagedConnection c = (ManagedConnection) iterator.next();
            if (c instanceof ZooKeeperManagedConnection) {
                connection = c;
            }
        }
        return connection;
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
    public ResourceAdapter getResourceAdapter() {
        return resourceAdapter;
    }

    /**
     * {@inheritDoc}
     */
    public void setResourceAdapter(ResourceAdapter resourceAdapter) {
        this.resourceAdapter = resourceAdapter;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }

        if (!(obj instanceof ZooKeeperManagedConnectionFactory)) {
            return false;
        }

        ZooKeeperManagedConnectionFactory factory = (ZooKeeperManagedConnectionFactory) obj;
        return Objects.equals(connectString, factory.connectString) &&
               Objects.equals(sessionTimeout, factory.sessionTimeout) &&
               canBeReadOnly == factory.canBeReadOnly;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int hashCode() {
        return Objects.hash(connectString, sessionTimeout, canBeReadOnly);
    }

}
