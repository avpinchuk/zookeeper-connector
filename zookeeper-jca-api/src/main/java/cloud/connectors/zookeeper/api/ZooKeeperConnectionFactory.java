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

import javax.resource.Referenceable;
import javax.resource.ResourceException;
import java.io.Serializable;

/**
 * ZookeeperConnectionFactory instance is a factory for ZooKeeper connection instances.
 *
 * <p><strong>Example:</strong>
 * <pre>
 *     &#64;ConnectionFactoryDefinition(
 *          name = "java:comp/env/ZooKeeper,
 *          interfaceName = "cloud.connectors.zookeeper.api.ZooKeeperConnectionFactory",
 *          resourceAdapter = "zookeeper-rar",
 *          minPoolSize = 5,
 *          properties = {
 *              "connectString=localhost:2181",
 *              "sessionTimeout=3000",
 *              "canBeReadOnly=false"
 *          }
 *     )
 *     public class Foo {
 *         ...
 *         &#64;Resource(lookup = "java:comp/env/ZooKeeper")
 *         private ZooKeeperConnectionFactory factory;
 *         ...
 *         public void bar() {
 *             ZooKeeperConnection connection = factory.getConnection();
 *             ...
 *         }
 *         ...
 *     }
 * </pre>
 *
 * @author alexa
 */
@SuppressWarnings("unused")
public interface ZooKeeperConnectionFactory extends Serializable, Referenceable {

    /**
     * Gets an application level  connection handle to the ZooKeeper server.
     *
     * @return {@link ZooKeeperConnection} instance connected to the ZooKeeper server
     * @throws ResourceException failed to get a connection to the ZooKeeper server
     */
    ZooKeeperConnection getConnection() throws ResourceException;

}
