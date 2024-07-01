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
 * 定义了一个用于节点发现的接口，实现了生命周期组件的特性。
 */
public interface Discovery extends LifecycleComponent<Discovery> {

    /**
     * 添加一个节点初始状态发现监听器。
     * 当节点发现过程完成并准备好初始状态时，该监听器会被通知。
     *
     * @param listener 要添加的监听器对象
     */
    void addListener(InitialStateDiscoveryListener listener);

    /**
     * 移除一个之前添加的节点初始状态发现监听器。
     *
     * @param listener 要移除的监听器对象
     */
    void removeListener(InitialStateDiscoveryListener listener);

    /**
     * 返回当前节点的描述信息。
     * 描述信息通常包括节点的名称、地址等。
     *
     * @return 节点描述的字符串
     */
    String nodeDescription();

    /**
     * 判断当前节点是否是集群中的主节点。
     *
     * @return 如果是主节点返回true，否则返回false
     */
    boolean firstMaster();

    /**
     * 发布集群状态信息。
     * 通常在集群状态发生变化时调用此方法。
     *
     * @param clusterState 要发布的集群状态对象
     */
    void publish(ClusterState clusterState);
}
