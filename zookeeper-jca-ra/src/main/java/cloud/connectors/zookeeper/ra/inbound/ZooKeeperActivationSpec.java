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

import cloud.connectors.zookeeper.api.ZooKeeperListener;
import org.apache.zookeeper.server.ZooKeeperServer;

import javax.resource.ResourceException;
import javax.resource.spi.Activation;
import javax.resource.spi.ActivationSpec;
import javax.resource.spi.ConfigProperty;
import javax.resource.spi.InvalidPropertyException;
import javax.resource.spi.ResourceAdapter;

/**
 * The ZooKeeper activation spec.
 *
 * <p>This holds the activation configuration information for a message endpoint.
 *
 * @author alexa
 */
@Activation(messageListeners = {ZooKeeperListener.class})
@SuppressWarnings({"unused", "RedundantThrows"})
public class ZooKeeperActivationSpec implements ActivationSpec {

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
     * Connect timeout in milliseconds. Will try to connect to the ZooKeeper indefinitely
     * if not specified.
     */
    @ConfigProperty(type = Integer.class)
    private Integer connectTimeout;

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
     * A path for the node for which we will tracking changes.
     */
    @ConfigProperty(type = String.class, defaultValue = "/")
    private String basePath = "/";

    /**
     * If {@code recursive} is {@code false} sets a persistent watcher on the given path which
     * does not get removed when triggered (i.e. it stays active until it is removed). This watcher
     * is triggered for {@link org.apache.zookeeper.Watcher.Event.EventType#NodeDataChanged NodeDataChanged}
     * and {@link org.apache.zookeeper.Watcher.Event.EventType#NodeChildrenChanged NodeChildrenChanged}
     * event types.
     *
     * <p>If {@code recursive} is {@code true} sets a persistent watcher on the given path which
     * does not get removed when triggered (i.e. it stays active until it is removed) and applies
     * not only to the registered path but all child paths recursively. This watcher is triggered for
     * {@link org.apache.zookeeper.Watcher.Event.EventType#NodeCreated NodeCreated},
     * {@link org.apache.zookeeper.Watcher.Event.EventType#NodeDeleted NodeDeleted} and
     * {@link org.apache.zookeeper.Watcher.Event.EventType#NodeDataChanged NodeDataChanged}
     * event types.
     */
    @ConfigProperty(type = Boolean.class, defaultValue = "false")
    private Boolean recursive = Boolean.FALSE;

    /**
     * The resource adapter
     */
    private ResourceAdapter resourceAdapter;

    /**
     * Get the {@code connectString}.
     * @return the connect string
     */
    public String getConnectString() {
        return connectString;
    }

    /**
     * Set the {@code connectString}.
     * @param connectString the connect string
     */
    public void setConnectString(String connectString) {
        this.connectString = connectString;
    }

    /**
     * Get the {@code connectTimeout}.
     * @return the connect timeout
     */
    public Integer getConnectTimeout() {
        return connectTimeout;
    }

    /**
     * Set the {@code connectTimeout}.
     * @param connectTimeout the connect timeout
     */
    public void setConnectTimeout(Integer connectTimeout) {
        this.connectTimeout = connectTimeout;
    }

    /**
     * Get the {@code sessionTimeout}.
     * @return the session timeout
     */
    public Integer getSessionTimeout() {
        return sessionTimeout;
    }

    /**
     * Set the {@code sessionTimeout}.
     * @param sessionTimeout the session timeout
     */
    public void setSessionTimeout(Integer sessionTimeout) {
        this.sessionTimeout = sessionTimeout;
    }

    /**
     * Get the {@code canBeReadOnly}.
     * @return the read-only flag
     */
    public Boolean isCanBeReadOnly() {
        return canBeReadOnly;
    }

    /**
     * Set the {@code canBeReadOnly}.
     * @param canBeReadOnly the read-only flag
     */
    public void setCanBeReadOnly(Boolean canBeReadOnly) {
        this.canBeReadOnly = canBeReadOnly;
    }

    /**
     * Get the {@code basePath}.
     * @return the base path
     */
    public String getBasePath() {
        return basePath;
    }

    /**
     * Set the {@code basePath}.
     * @param basePath the base path
     */
    public void setBasePath(String basePath) {
        this.basePath = basePath;
    }

    /**
     * Get the {@code recursive}.
     * @return the recursive flag
     */
    public Boolean isRecursive() {
        return recursive;
    }

    /**
     * Set the {@code recursive}.
     * @param recursive the recursive flag
     */
    public void setRecursive(Boolean recursive) {
        this.recursive = recursive;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void validate() throws InvalidPropertyException {
        if (!basePath.startsWith("/")) {
            throw new InvalidPropertyException("basePath must starts with '/'");
        }
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
    @Override
    public void setResourceAdapter(ResourceAdapter resourceAdapter) throws ResourceException {
        this.resourceAdapter = resourceAdapter;
    }

}
