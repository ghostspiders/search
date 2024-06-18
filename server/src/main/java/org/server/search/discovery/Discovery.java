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
import org.server.search.util.component.LifecycleComponent;


/**
 * 节点发现接口，负责集群中的节点发现和主节点选举。
 * <p>
 * 实现此接口的类必须实现LifecycleComponent接口，以管理组件的生命周期。
 */
public interface Discovery extends LifecycleComponent<Discovery> {

    /**
     * 添加一个初始状态发现监听器。
     * 这个监听器在节点发现过程的初始阶段被调用，用于处理节点加入集群的逻辑。
     *
     * @param listener 要添加的初始状态发现监听器
     */
    void addListener(InitialStateDiscoveryListener listener);

    /**
     * 移除一个初始状态发现监听器。
     *
     * @param listener 要移除的初始状态发现监听器
     */
    void removeListener(InitialStateDiscoveryListener listener);

    /**
     * 返回当前节点的描述信息。
     * 这个描述信息通常包含了节点的基本信息，如节点名称、地址等。
     *
     * @return 当前节点的描述信息
     */
    String nodeDescription();

    /**
     * 判断当前节点是否是集群中的首个主节点。
     * 如果集群中还没有主节点，这个方法会返回true，表示当前节点可以开始选举过程。
     *
     * @return 如果当前节点是首个主节点，则返回true
     */
    boolean firstMaster();

    /**
     * 发布集群状态到集群中的其他节点。
     * 当集群状态发生变化时，需要通过这个方法将新的状态发布给集群中的其他节点。
     *
     * @param clusterState 要发布的集群状态
     */
    void publish(ClusterState clusterState);
}