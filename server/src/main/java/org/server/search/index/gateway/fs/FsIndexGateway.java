/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.server.search.index.gateway.fs;

import com.google.inject.Inject;
import org.server.search.env.Environment;
import org.server.search.gateway.Gateway;
import org.server.search.gateway.fs.FsGateway;
import org.server.search.index.AbstractIndexComponent;
import org.server.search.index.Index;
import org.server.search.index.gateway.IndexGateway;
import org.server.search.index.gateway.IndexShardGateway;
import org.server.search.index.settings.IndexSettings;
import org.server.search.util.Strings;
import org.server.search.util.io.FileSystemUtils;
import org.server.search.util.settings.Settings;

import java.io.File;

/**
 * @author kimchy (Shay Banon)
 */
public class FsIndexGateway extends AbstractIndexComponent implements IndexGateway {

    private final Environment environment;

    private final Gateway gateway;

    private final String location;

    private File indexGatewayHome;

    @Inject public FsIndexGateway(Index index, @IndexSettings Settings indexSettings, Environment environment, Gateway gateway) {
        super(index, indexSettings);
        this.environment = environment;
        this.gateway = gateway;

        String location = componentSettings.get("location");
        if (location == null) {
            if (gateway instanceof FsGateway) {
                indexGatewayHome = new File(((FsGateway) gateway).gatewayHome(), index().name());
            } else {
                indexGatewayHome = new File(new File(environment.workWithClusterFile(), "gateway"), index().name());
            }
            location = Strings.cleanPath(indexGatewayHome.getAbsolutePath());
        } else {
            indexGatewayHome = new File(location);
        }
        this.location = location;
        indexGatewayHome.mkdirs();
    }

    @Override public Class<? extends IndexShardGateway> shardGatewayClass() {
        return FsIndexShardGateway.class;
    }

    @Override public void delete() {
        FileSystemUtils.deleteRecursively(indexGatewayHome, false);
    }

    @Override public void close() {
    }

    public File indexGatewayHome() {
        return this.indexGatewayHome;
    }
}
