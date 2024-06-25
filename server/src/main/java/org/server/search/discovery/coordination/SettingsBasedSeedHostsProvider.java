/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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

package org.server.search.discovery.coordination;

import cn.hutool.core.collection.CollUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.server.search.transport.TransportService;
import org.server.search.util.settings.Settings;

import java.util.List;

import static java.util.Collections.emptyList;


public class SettingsBasedSeedHostsProvider{

    private static final Logger logger = LogManager.getLogger(SettingsBasedSeedHostsProvider.class);

    public  List<String> LEGACY_DISCOVERY_ZEN_PING_UNICAST_HOSTS_SETTING;

    public List<String> DISCOVERY_SEED_HOSTS_SETTING;

    private static final int LIMIT_FOREIGN_PORTS_COUNT = 1;
    private static final int LIMIT_LOCAL_PORTS_COUNT = 5;

    private final List<String> configuredHosts;
    private final int limitPortCounts;

    public SettingsBasedSeedHostsProvider(Settings settings, TransportService transportService) {
        LEGACY_DISCOVERY_ZEN_PING_UNICAST_HOSTS_SETTING = settings.getListStr("discovery.zen.ping.unicast.hosts",emptyList());
        DISCOVERY_SEED_HOSTS_SETTING = settings.getListStr("discovery.seed_hosts",emptyList());



        if (CollUtil.isNotEmpty(LEGACY_DISCOVERY_ZEN_PING_UNICAST_HOSTS_SETTING)) {
            if (CollUtil.isNotEmpty(DISCOVERY_SEED_HOSTS_SETTING)) {
                throw new IllegalArgumentException("it is forbidden to set both ["
                    + "discovery.seed_hosts" + "] and ["
                    + "discovery.zen.ping.unicast.hosts" + "]");
            }
            configuredHosts = LEGACY_DISCOVERY_ZEN_PING_UNICAST_HOSTS_SETTING;
            limitPortCounts = LIMIT_FOREIGN_PORTS_COUNT;
        } else if (CollUtil.isNotEmpty(DISCOVERY_SEED_HOSTS_SETTING)) {
            configuredHosts = DISCOVERY_SEED_HOSTS_SETTING;
            limitPortCounts = LIMIT_FOREIGN_PORTS_COUNT;
        } else {
            configuredHosts = transportService.getLocalAddresses();
            limitPortCounts = LIMIT_LOCAL_PORTS_COUNT;
        }

        logger.debug("using initial hosts {}", configuredHosts);
    }
}
