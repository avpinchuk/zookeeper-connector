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

/**
 * Marker interface for a class that is a Message Driven Bean for ZooKeeper.
 *
 * <p><strong>Example:</strong>
 * <pre>
 *     &#64;@MessageDriven(activationConfig = {
 *         &#64;ActivationConfigProperty(propertyName = "connectString", propertyValue = "localhost:2181"),
 *         &#64;ActivationConfigProperty(propertyName = "connectTimeout", propertyValue = "3000"),
 *         &#64;ActivationConfigProperty(propertyName = "sessionTimeout", propertyValue = "3000"),
 *         &#64;ActivationConfigProperty(propertyName = "canBeReadOnly", propertyValue = "false"),
 *         &#64;ActivationConfigProperty(propertyName = "basePath", propertyValue = "/"),
 *         &#64;ActivationConfigProperty(propertyName = "recursive", propertyValue = "false")
 *     })
 *     public class ZooKeeperMDB implements ZooKeeperListener {
 *         ...
 *         &#64;OnZooKeeperEvent
 *         public void onEvent(WatchedEvent event) {
 *             ...
 *         }
 *         ...
 *     }
 * </pre>
 *
 * @author alexa
 */
public interface ZooKeeperListener {

}
