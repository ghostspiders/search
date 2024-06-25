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

package org.server.search.discovery;

import org.server.search.cluster.ClusterState;
import org.server.search.discovery.coordination.ClusterStatePublisher;
import org.server.search.util.component.LifecycleComponent;


/**
 * 节点发现接口，负责集群中的节点发现和主节点选举。
 * <p>
 * 实现此接口的类必须实现LifecycleComponent接口，以管理组件的生命周期。
 */
public interface Discovery extends LifecycleComponent<Discovery>, ClusterStatePublisher {

    /**
     * 触发初始的加入周期。
     * <p>
     * 这个方法启动节点加入集群的初始过程，通常用于节点启动时寻找并加入现有的集群。
     * <p>
     * 在这个过程中，节点会尝试与集群中的其他节点建立连接，并参与到主节点选举和集群状态同步中。
     * <p>
     * 这个方法通常在节点启动时被调用一次，但在某些情况下，如集群分裂恢复后，可能会再次触发。
     */
    void startInitialJoin();
}