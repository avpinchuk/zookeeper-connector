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

import org.apache.zookeeper.AddWatchMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import javax.resource.ResourceException;
import javax.resource.spi.endpoint.MessageEndpointFactory;
import javax.resource.spi.work.Work;
import javax.resource.spi.work.WorkManager;
import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A main event handler class. Will get various events from the ZooKeeper server
 * it connects to.
 *
 * <p>Will start in a separate thread by a {@code WorkManager}.
 *
 * @author alexa
 */
public class ZooKeeperWatcher implements Watcher, Work {

    private static final Logger logger = Logger.getLogger(ZooKeeperWatcher.class.getName());

    private final MessageEndpointFactory messageEndpointFactory;
    private final WorkManager workManager;

    /**
     * A comma separated host:port pairs, each corresponds to a ZooKeeper server,
     * e.g. "127.0.0.1:3000,127.0.0.1:3001,127.0.0.1:3002". If the optional chroot
     * suffix is used, the example would look like
     * "127.0.0.1:3000,127.0.0.1:3001,127.0.0.1:3002/app/a", where the client would
     * be rooted at "/app/a" and all paths would be relative to this root, i.e.
     * getting/setting/etc... "/foo/bar" would result in operations being run on
     * "/app/a/foo/bar" (from the server perspective).
     */
    private final String connectString;

    /**
     * Connect timeout in milliseconds. Will try to connect to the ZooKeeper indefinitely
     * if not specified.
     */
    private final Integer connectTimeout;

    /**
     * Session timeout in milliseconds.
     */
    private final int sessionTimeout;

    /**
     * Whether the created client is allowed to go to read-only mode in case of partitioning.
     * Read-only mode basically means that if the client can't find any majority servers
     * but there's partitioned server it could reach, it connects to one in read-only mode,
     * i.e. read requests are allowed while write requests are not. It continues seeking
     * for majority in the background.
     */
    private final boolean canBeReadOnly;

    /**
     * A path for the node for which we will tracking changes.
     */
    private final String basePath;

    /**
     * ZooKeeper watch mode.
     */
    private final AddWatchMode watchMode;

    private volatile ZooKeeper zooKeeper;

    public ZooKeeperWatcher(MessageEndpointFactory messageEndpointFactory,
                            ZooKeeperActivationSpec activationSpec,
                            WorkManager workManager) {
        this.messageEndpointFactory = messageEndpointFactory;
        this.workManager = workManager;

        this.connectString = activationSpec.getConnectString();
        this.connectTimeout = activationSpec.getConnectTimeout();
        this.sessionTimeout = activationSpec.getSessionTimeout();
        this.canBeReadOnly = activationSpec.isCanBeReadOnly();
        this.basePath = activationSpec.getBasePath();
        this.watchMode = activationSpec.isRecursive()
                         ? AddWatchMode.PERSISTENT_RECURSIVE
                         : AddWatchMode.PERSISTENT;
    }

    /**
     * Process an incoming ZooKeeper event. We process only a data and children
     * events. Connection related events are ignored. Each event proceed in a
     * separate thread.
     *
     * @param watchedEvent an incoming event
     */
    @Override
    public void process(WatchedEvent watchedEvent) {
        try {
            switch (watchedEvent.getState()) {
                case SyncConnected:
                    if (watchedEvent.getType() != Event.EventType.None) {
                        // process a data or children event
                        workManager.scheduleWork(new ZooKeeperWork(watchedEvent, messageEndpointFactory));
                    } else {
                        // Connection to server was lost. We are reconnect within
                        // session timeout and will be operate in current session.
                        // This is because initial connection (initial SyncConnected
                        // event) was make in the connectAndWatch() method
                        logger.log(Level.WARNING, "Connection was lost. Reconnect to the ZooKeeper Server");
                    }
                    break;
                case Expired:
                    // Session was expired. We create new ZooKeeper object and re-register
                    // this watcher with it
                    logger.log(Level.WARNING, "Session expired. Create new ZooKeeper session");
                    connectAndWatch(connectTimeout);
                    break;
                default:
                    // Ignore ZooKeeper lifecycle events. Log them at FINEST logging level if any
                    logger.log(Level.FINEST, "ZooKeeper Server fire an event: {0}", watchedEvent);
                    break;
            }
        } catch (KeeperException | InterruptedException | IOException | ResourceException e) {
            logger.log(Level.SEVERE, null, e);
        }
    }

    /**
     * Creates ZooKeeper instance and set register the current watcher with it.
     */
    @Override
    public void run() {
        try {
            connectAndWatch(connectTimeout);
        } catch (KeeperException | InterruptedException | IOException e) {
            logger.log(Level.SEVERE, null, e);
        }
    }

    /**
     * {@inheritDoc}
     *
     * <p><strong>This method is does nothing.</strong>
     */
    @Override
    public void release() {
        // do nothing
    }

    /**
     * Removes all registered watchers and closes current ZooKeeper instance.
     *
     * @throws ResourceException if the server returns a non-zero error code or
     * the operation was interrupted
     */
    public void close() throws ResourceException {
        try {
            zooKeeper.removeAllWatches(basePath, WatcherType.Any, true);
            zooKeeper.close();
        } catch (KeeperException | InterruptedException e) {
            throw new ResourceException("Unable to close ZooKeeper client connection", e);
        }
    }

    /**
     * Creates ZooKeeper objects, wait for connection and register working watcher.
     * Will wait up to {@code timeout} milliseconds or indefinitely if {@code timeout}
     * is {@code null}.
     *
     * @param timeout the connect timeout, {@code null} if wait indefinitely
     * @throws IOException if an I/O error occurred while connecting
     * @throws InterruptedException if an operation has been interrupted
     * @throws KeeperException if an connection timeout exceed or watcher
     * registration failed
     */
    private void connectAndWatch(Integer timeout) throws IOException, InterruptedException, KeeperException {
        CountDownLatch keeperLatch = new CountDownLatch(1);
        // Initial watcher for observe an initial SyncConnected event.
        // When this event fired we are connected
        Watcher watcher = watchedEvent -> {
            if (watchedEvent.getState() == Event.KeeperState.SyncConnected) {
                keeperLatch.countDown();
            }
        };

        zooKeeper = new ZooKeeper(connectString, sessionTimeout, watcher, canBeReadOnly);

        // await connection
        if (timeout == null) {
            // wait indefinitely
            keeperLatch.await();
        } else {
            if (!keeperLatch.await(connectTimeout, TimeUnit.MILLISECONDS)) {
                // timeout exceed but we are not connected
                throw new KeeperException.OperationTimeoutException();
            }
        }

        // Remove initial connection watcher
        zooKeeper.register(null);
        // set main working watcher
        zooKeeper.addWatch(basePath, this, watchMode);
    }

}
