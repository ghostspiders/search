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

/**
 * 一个发现节点（DiscoveryNode）代表集群中的一个节点。
 * <p>
 * 此类用于存储有关集群节点的信息，包括节点的网络地址、唯一标识符、版本信息等。
 * 这些信息在节点发现、集群状态管理和节点间通信时使用。
 */
public class DiscoveryNode{

    /**
     * Enum that holds all the possible roles that that a node can fulfill in a cluster.
     * Each role has its name and a corresponding abbreviation used by cat apis.
     */
    public enum Role {
        MASTER("master", "m"),
        DATA("data", "d"),
        INGEST("ingest", "i");

        private final String roleName;
        private final String abbreviation;

        Role(String roleName, String abbreviation) {
            this.roleName = roleName;
            this.abbreviation = abbreviation;
        }

        public String getRoleName() {
            return roleName;
        }

        public String getAbbreviation() {
            return abbreviation;
        }
    }
}
