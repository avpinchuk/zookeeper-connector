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

package cloud.connectors.zookeeper.api;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;

import javax.resource.ResourceException;
import java.io.Closeable;
import java.util.List;

/**
 * Represents an application-level handle that is used by an a client to access
 * the underlying physical connection. The actual physical connection associated
 * with a handle is represented by {@code ZooKeeperManagedConnection} instance.
 *
 * <p>A client gets a {@code ZooKeeperConnection} instance by using the
 * {@code getConnection()} method on a {@code ZooKeeperConnectionFactory} instance.
 *
 * <p><strong>Example:</strong>
 * <pre>
 *      ...
 *      &#64;Resource(lookup = "java:comp/env/ZooKeeper")
 *      ZooKeeperConnectionFactory connectionFactory;
 *      ...
 *      ZooKeeperConnection = connectionFactory.getConnection();
 *      ...
 * </pre>
 *
 * @author alexa
 */
public interface ZooKeeperConnection extends Closeable {

    /**
     * Create a node with the given {@code path}. The node data will be given {@code data},
     * and node acl will be the given {@code acl}.
     *
     * <p>The {@code createMode} argument specifies whether the created node will be
     * ephemeral or not. An ephemeral node will be removed by the ZooKeeper automatically
     * when the session associated with the creation of the node expires.
     *
     * <p>The {@code createMode} argument can also specify to create a sequential node.
     * The actual path name of a sequential node will be the given path with suffix
     * <em>i</em> where <em>i</em> is the current sequential number of the node. The
     * sequence number is always fixed length of ten zero-padded digits. Once such a
     * node is created, the sequential number will be incremented by one.
     *
     * <p>This operation, if successful, will trigger all watches left on this node for the
     * event type {@link org.apache.zookeeper.Watcher.Event.EventType#NodeCreated NodeCreated}
     * and the watches left on the parent node of the this node for the event type
     * {@link org.apache.zookeeper.Watcher.Event.EventType#NodeChildrenChanged NodeChildrenChanged}.
     *
     * <p>The maximum allowable size of the data array is {@code 1} MB ({@code 1 048 576} bytes).
     * Arrays larger than this will cause an error.
     *
     * @param path the path for the node
     * @param data the initial data for the node
     * @param acl the acl for the node
     * @param createMode specifying whether the node to be created is ephemeral and/or sequential
     * @return the actual path of the created node
     * @throws javax.resource.spi.EISSystemException if the server returns a non-zero error code
     * @throws javax.resource.spi.CommException if the operation was interrupted
     * @throws IllegalArgumentException if an invalid path is specified
     * @see org.apache.zookeeper.ZooDefs.Perms ZooDefs.Perms
     * @see org.apache.zookeeper.ZooDefs.Ids ZooDefs.Ids
     */
    String create(String path, byte[] data, List<ACL> acl, CreateMode createMode) throws ResourceException;

    /**
     * Create a node with the given {@code path} and returns the {@code stat} of the node.
     * The node data will be given {@code data}, and node acl will be the given {@code acl}.
     *
     * <p>The {@code createMode} arguments specifies whether the created node will be
     * ephemeral or not. An ephemeral node will be removed by the ZooKeeper automatically
     * when the session associated with the creation of the node expires.
     *
     * <p>The {@code createMode} argument can also specify to create a sequential node.
     * The actual path name of a sequential node will be the given path with suffix
     * <em>i</em> where <em>i</em> is the current sequential number of the node. The
     * sequence number is always fixed length of ten zero-padded digits. Once such a
     * node is created, the sequential number will be incremented by one.
     *
     * <p>This operation, if successful, will trigger all watches left on this node for the
     * event type {@link org.apache.zookeeper.Watcher.Event.EventType#NodeCreated NodeCreated}
     * and the watches left on the parent node of the this node for the event type
     * {@link org.apache.zookeeper.Watcher.Event.EventType#NodeChildrenChanged NodeChildrenChanged}.
     *
     * <p>The maximum allowable size of the data array is {@code 1} MB ({@code 1 048 576} bytes).
     * Arrays larger than this will cause an error.
     *
     * @param path the path for the node
     * @param data the initial data for the node
     * @param acl the acl for the node
     * @param createMode specifying whether the node to be created is ephemeral and/or sequential
     * @param stat the output {@link Stat} object
     * @return the actual path of the created node
     * @throws javax.resource.spi.EISSystemException if the server returns a non-zero error code
     * @throws javax.resource.spi.CommException if the operation was interrupted
     * @throws IllegalArgumentException if an invalid path is specified
     * @see org.apache.zookeeper.ZooDefs.Perms ZooDefs.Perms
     * @see org.apache.zookeeper.ZooDefs.Ids ZooDefs.Ids
     */
    String create(String path, byte[] data, List<ACL> acl, CreateMode createMode, Stat stat) throws ResourceException;

    /**
     * The same as {@link #create(String, byte[], List, CreateMode, Stat)} but allow
     * for specifying {@code TTL}  when mode is {@link CreateMode#PERSISTENT_WITH_TTL} or
     * {@link CreateMode#PERSISTENT_SEQUENTIAL_WITH_TTL}. If the node has not been
     * modified within the given TTL, it will be deleted once is has no children.
     * The TTL unit is milliseconds and must be greater than {@code 0} and less than
     * or equal to {@link org.apache.zookeeper.server.EphemeralType#maxValue() EphemeralType.maxValue()}
     * for the {@link org.apache.zookeeper.server.EphemeralType#TTL EphemeralType.TTL}.
     *
     * @param path the path for the node
     * @param data the initial data for the node
     * @param acl the acl for the node
     * @param createMode specifying whether the node to be created is ephemeral and/or sequential
     * @param stat the output {@link Stat} object
     * @param ttl the time to live of a created node
     * @return the actual created node
     * @throws javax.resource.spi.EISSystemException if the server returns a non-zero error code
     * @throws javax.resource.spi.CommException if the operation was interrupted
     * @throws IllegalArgumentException if an invalid path is specified
     * @see #create(String, byte[], List, CreateMode, Stat)
     * @see org.apache.zookeeper.ZooDefs.Perms ZooDefs.Perms
     * @see org.apache.zookeeper.ZooDefs.Ids ZooDefs.Ids
     */
    String create(String path, byte[] data, List<ACL> acl, CreateMode createMode, Stat stat, long ttl) throws ResourceException;

    /**
     * Delete the node with the given {@code path}.
     *
     * <p>This operation, if successful, will trigger all watches left on this node for the
     * event type {@link org.apache.zookeeper.Watcher.Event.EventType#NodeDeleted NodeDeleted}
     * and the watches left on the parent node of the this node for the event type
     * {@link org.apache.zookeeper.Watcher.Event.EventType#NodeChildrenChanged NodeChildrenChanged}.
     *
     * <p>This method is same as:
     * <pre>
     *     delete(path, -1);
     * </pre>
     *
     * @param path the path of the node to be deleted
     * @throws javax.resource.spi.EISSystemException if the server returns a non-zero error code
     * @throws javax.resource.spi.CommException if the operation was interrupted
     * @throws IllegalArgumentException if an invalid path is specified
     * @see #delete(String, int)
     */
    void delete(String path) throws ResourceException;

    /**
     * Delete the node with the given {@code path}. The call will succeed if such a node exists,
     * and the given {@code version} matches the node's version (if given version is {@code -1},
     * it matches any node's version.
     *
     * <p>This operation, if successful, will trigger all watches left on this node for the
     * event type {@link org.apache.zookeeper.Watcher.Event.EventType#NodeDeleted NodeDeleted}
     * and the watches left on the parent node of the this node for the event type
     * {@link org.apache.zookeeper.Watcher.Event.EventType#NodeChildrenChanged NodeChildrenChanged}.
     *
     * @param path the path of the node to be deleted
     * @param version the expected node version
     * @throws javax.resource.spi.EISSystemException if the server returns an error with a non-zero error code
     * @throws javax.resource.spi.CommException if the operation was interrupted
     * @throws IllegalArgumentException if an invalid path is specified
     */
    void delete(String path, int version) throws ResourceException;

    /**
     * Returns the stat of the node of the given {@code path}. Returns {@code null} if no such
     * node exists.
     *
     * @param path the node path
     * @return the stat of the node of the given path or {@code null} if no such a node exists
     * @throws javax.resource.spi.EISSystemException if the server returns a non-zero error code
     * @throws javax.resource.spi.CommException if the operation was interrupted
     * @throws IllegalArgumentException if an invalid path is specified
     */
    Stat exists(String path) throws ResourceException;

    /**
     * Returns the {@code ACL} of the node of the given {@code path}.
     *
     * <p>This method is same as:
     * <pre>
     *     exists(path, null);
     * </pre>
     *
     * @param path the given path for the node
     * @return the ACL array of the given node
     * @throws javax.resource.spi.EISSystemException if the server returns a non-zero error code
     * @throws javax.resource.spi.CommException if the operation was interrupted
     * @throws IllegalArgumentException if an invalid path is specified
     * @see #getACL(String, Stat)
     */
    List<ACL> getACL(String path) throws ResourceException;

    /**
     * Returns the {@code ACL} and {@code stat} of the node of the given {@code path}.
     *
     * @param path the given path for the node
     * @param stat the stat of the node wil be copied to this parameter if not {@code null}
     * @return the ACL array of the given node
     * @throws javax.resource.spi.EISSystemException if the server returns a non-zero error code
     * @throws javax.resource.spi.CommException if the operation was interrupted
     * @throws IllegalArgumentException if an invalid path is specified
     */
    List<ACL> getACL(String path, Stat stat) throws ResourceException;

    /**
     * Synchronously gets all numbers of a children nodes under a specified {@code path}.
     *
     * @param path the given path for the node
     * @return the number of the children nodes
     * @throws javax.resource.spi.EISSystemException if the server returns a non-zero error code
     * @throws javax.resource.spi.CommException if the operation was interrupted
     * @throws IllegalArgumentException if an invalid path is specified
     */
    int getAllChildrenNumber(String path) throws ResourceException;

    /**
     * Returns the list of the children of the node of the given {@code path}.
     *
     * <p>The list of children returned is not sorted and no guarantee is a provided
     * as to its natural or lexical order.
     *
     * @param path the given path for the node
     * @return an unordered list of children of the node with the given path
     * @throws javax.resource.spi.EISSystemException if the server returns a non-zero error code
     * @throws javax.resource.spi.CommException if the operation was interrupted
     * @throws IllegalArgumentException if an invalid path is specified
     */
    List<String> getChildren(String path) throws ResourceException;

    /**
     * For the given node {@code path} return the {@code stat} and a children list.
     *
     * <p>The list of children returned is not sorted and no guarantee is a provided
     * as to its natural or lexical order.
     *
     * @param path the given path for the node
     * @param stat stat of the node designated by path
     * @return an unordered list of children of the node with the given path
     * @throws javax.resource.spi.EISSystemException if the server returns a non-zero error code
     * @throws javax.resource.spi.CommException if the operation was interrupted
     * @throws IllegalArgumentException if an invalid path is specified
     */
    List<String> getChildren(String path, Stat stat) throws ResourceException;

    /**
     * Returns the data of the node of the given path.
     *
     * <p>This method is same as:
     * <pre>
     *     getData(path, null);
     * </pre>
     *
     * @param path the given path for the node
     * @return the data of the node
     * @throws javax.resource.spi.EISSystemException if the server returns a non-zero error code
     * @throws javax.resource.spi.CommException if the operation was interrupted
     * @throws IllegalArgumentException if an invalid path is specified
     * @see #getData(String, Stat)
     */
    byte[] getData(String path) throws ResourceException;

    /**
     * Returns the data and the stat of the node of the given path.
     *
     * @param path the given path for the node
     * @param stat stat of the node designated by path
     * @return the data of the node
     * @throws javax.resource.spi.EISSystemException if the server returns a non-zero error code
     * @throws javax.resource.spi.CommException if the operation was interrupted
     * @throws IllegalArgumentException if an invalid path is specified
     */
    byte[] getData(String path, Stat stat) throws  ResourceException;

    /**
     * Synchronously returns all the ephemeral nodes created by this session.
     *
     * @return a list of the all the ephemeral nodes created by this session
     * @throws javax.resource.spi.EISSystemException if the server returns a non-zero error code
     * @throws javax.resource.spi.CommException if the operation was interrupted
     */
    List<String> getEphemerals() throws ResourceException;

    /**
     * Synchronously gets all the ephemeral nodes matching {@code prefixPath} created
     * by this session. If {@code prefixPath} is "{@code /}" then it returns all the
     * ephemeral nodes.
     *
     * @param prefixPath the prefix path the nodes starts with
     * @return a list of the ephemeral nodes matching the given prefix path
     * @throws javax.resource.spi.EISSystemException if the server returns a non-zero error code
     * @throws javax.resource.spi.CommException if the operation was interrupted
     * @throws IllegalArgumentException if an invalid prefix path is specified
     */
    List<String> getEphemerals(String prefixPath) throws ResourceException;

    /**
     * Set the {@code ACL} for the node of the given {@code path} if such a node exists.
     *
     * <p>This method is same as:
     * <pre>
     *     setACL(path, acl, -1);
     * </pre>
     *
     * @param path the given path for the node
     * @param acl the given acl for the node
     * @return the state of the node
     * @throws javax.resource.spi.EISSystemException if the server returns a non-zero error code
     * @throws javax.resource.spi.CommException if the operation was interrupted
     * @throws IllegalArgumentException if an invalid path is specified
     * @see #setACL(String, List, int)
     */
    Stat setACL(String path, List<ACL> acl) throws ResourceException;

    /**
     * Set the {@code ACL} for the node of the given {@code path} if such a node
     * exists and the given {@code version} matches the acl version of the node.
     *
     * @param path the given path for the node
     * @param acl the given acl for the node
     * @param version the given acl version of the node
     * @return the state of the node
     * @throws javax.resource.spi.EISSystemException if the server returns a non-zero error code
     * @throws javax.resource.spi.CommException if the operation was interrupted
     * @throws IllegalArgumentException if an invalid path is specified
     */
    Stat setACL(String path, List<ACL> acl, int version) throws ResourceException;

    /**
     * Sets the {@code data} for the node of the given {@code path} if such a node exists.
     *
     * <p>The maximum allowable size of the {@code data} array is {@code 1} MB
     * ({@code 1 048 576} bytes). Arrays larger than this will cause an error.
     *
     * <p>This operation, if successful, will trigger all watches left on this node for the
     * event type {@link org.apache.zookeeper.Watcher.Event.EventType#NodeDataChanged NodeDataChanged}.
     *
     * <p>This method is same as:
     * <pre>
     *     setData(path, data, -1);
     * </pre>
     *
     * @param path the given path for the node
     * @param data the data to be set
     * @return the state of the node
     * @throws javax.resource.spi.EISSystemException if the server returns a non-zero error code
     * @throws javax.resource.spi.CommException if the operation was interrupted
     * @throws IllegalArgumentException if an invalid path is specified
     * @see #setData(String, byte[], int)
     */
    Stat setData(String path, byte[] data) throws ResourceException;

    /**
     * Sets the {@code data} for the node of the given {@code path} if such a node
     * exists and the given {@code version} matches the version of the node (if the
     * given version is {@code -1}, it matches any version of the node).
     *
     * <p>This operation, if successful, will trigger all watches left on this node for the
     * event type {@link org.apache.zookeeper.Watcher.Event.EventType#NodeDataChanged NodeDataChanged}.
     *
     * <p>The maximum allowable size of the {@code data} array is {@code 1} MB
     * ({@code 1 048 576} bytes). Arrays larger than this wil cause an error.
     *
     * @param path the given path for the node
     * @param data the data to be set
     * @param version the expected version
     * @return the state of the node
     * @throws javax.resource.spi.EISSystemException if the server returns a non-zero error code
     * @throws javax.resource.spi.CommException if the operation was interrupted
     * @throws IllegalArgumentException if an invalid path is specified
     */
    Stat setData(String path, byte[] data, int version) throws ResourceException;

    /**
     * Closes the connection handle at application level. A client should not use
     * a closed or inactive connection to interact with the ZooKeeper Server. This
     * will cause an {@link javax.resource.spi.IllegalStateException}.
     */
    @Override
    void close();

}
