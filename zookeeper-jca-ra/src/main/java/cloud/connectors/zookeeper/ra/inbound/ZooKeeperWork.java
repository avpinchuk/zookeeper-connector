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

import cloud.connectors.zookeeper.api.OnZooKeeperEvent;
import org.apache.zookeeper.WatchedEvent;

import javax.resource.ResourceException;
import javax.resource.spi.endpoint.MessageEndpoint;
import javax.resource.spi.endpoint.MessageEndpointFactory;
import javax.resource.spi.work.Work;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * This models a {@code Work} instance that would be executed by a
 * {@code WorkManager} upon submission. Create a message endpoint
 * instance and consume a ZooKeeper event.
 *
 * @author alexa
 */
public class ZooKeeperWork implements Work {

    private static final Logger logger = Logger.getLogger(ZooKeeperWork.class.getName());

    private static final ClassValue<Method> methodCache = new ClassValue<Method>() {
        @Override
        protected Method computeValue(Class<?> type) {
            Method method = null;
            for (Method m : type.getMethods()) {
                if (m.isAnnotationPresent(OnZooKeeperEvent.class) &&
                    m.getParameterCount() == 1 &&
                    m.getParameterTypes()[0] == WatchedEvent.class) {
                    method = m;
                    break;
                }
            }
            return method;
        }
    };

    private final WatchedEvent event;
    private final MessageEndpointFactory endpointFactory;
    private MessageEndpoint endpoint;
    private final ReentrantLock endpointLock;


    public ZooKeeperWork(WatchedEvent event, MessageEndpointFactory endpointFactory) {
        this.event = event;
        this.endpointFactory = endpointFactory;
        this.endpointLock = new ReentrantLock();
    }

    /**
     * Creates a message endpoint instance and consume ZooKeeper event.
     */
    @Override
    public void run() {
        try {
            Method method = methodCache.get(endpointFactory.getEndpointClass());
            if (method != null) {
                endpoint = endpointFactory.createEndpoint(null);
                endpoint.beforeDelivery(method);
                if (event != null) {
                    method.invoke(endpoint, event);
                }
                endpoint.afterDelivery();
            }
        } catch (ResourceException | NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
            logger.log(Level.SEVERE, null, e);
        } finally {
            if (endpoint != null) {
                endpoint.release();
                endpoint = null;
            }
        }
    }

    /**
     * Releases a message endpoint.
     *
     * <p>{@inheritDoc}
     */
    @Override
    public void release() {
        // because this may be invoked in
        // different thread we release
        // endpoint under lock
        endpointLock.lock();
        try {
            if (endpoint != null) {
                endpoint.release();
                endpoint = null;
            }
        } finally {
            endpointLock.unlock();
        }
    }

}
