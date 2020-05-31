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

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Marker annotation to indicate the method to be called on an Message Driven Bean
 * when an data event is arrived from ZooKeeper. Method has exactly one argument
 * with type {@link org.apache.zookeeper.WatchedEvent WatchedEvent}.
 *
 * <p><strong>Example:</strong>
 * <pre>
 *     ...
 *     &#64;OnZooKeeperEvent
 *     public void onEvent(WatchedEvent event) {
 *         ...
 *     }
 *     ...
 * </pre>
 *
 * @author alexa
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface OnZooKeeperEvent {

}
