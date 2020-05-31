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

import org.apache.zookeeper.Version;

import javax.resource.ResourceException;
import javax.resource.spi.ManagedConnectionMetaData;

/**
 * Provides information about the underlying ZooKeeper client currently.
 *
 * <p>The method ManagedConnection.getMetaData returns an instance of this class.
 *
 * @author alexa
 */
@SuppressWarnings("RedundantThrows")
public class ZooKeeperManagedConnectionMetadata implements ManagedConnectionMetaData {

    /**
     * Returns the ZooKeeper client name.
     *
     * @return the client name
     */
    @Override
    public String getEISProductName() throws ResourceException {
        return "Apache ZooKeeper Client";
    }

    /**
     * Returns the ZooKeeper client version.
     *
     * @return the client version
     */
    @Override
    public String getEISProductVersion() throws ResourceException {
        return Version.getFullVersion();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int getMaxConnections() throws ResourceException {
        return 0;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getUserName() throws ResourceException {
        return "";
    }
    
}
