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

package org.server.search.cluster.routing;

import org.server.search.index.shard.ShardId;
import org.server.search.util.io.Streamable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;

 
public interface ShardRouting extends Streamable, Serializable {
    /**
     * 返回分片所属的索引名称。
     * @return 索引名称字符串。
     */
    String index();

    /**
     * 返回分片在其索引内的唯一标识符。
     * @return 分片ID，作为整数返回。
     */
    int id();

    /**
     * 检查分片是否当前未分配，即它尚未被分配到任何节点上。
     * @return 如果分片未分配，返回true，否则返回false。
     */
    boolean unassigned();

    /**
     * 检查分片是否正在初始化过程中。
     * @return 如果分片正在初始化，返回true，否则返回false。
     */
    boolean initializing();

    /**
     * 检查分片是否已启动，并且准备好接受读写操作。
     * @return 如果分片已启动，返回true，否则返回false。
     */
    boolean started();

    /**
     * 检查分片是否正在从一个节点重新定位到另一个节点。
     * @return 如果分片正在重新定位，返回true，否则返回false。
     */
    boolean relocating();

    /**
     * 检查分片是否正在重新定位或已经启动。
     * 此方法返回true表示分片处于活跃状态。
     * @return 如果分片活跃（正在重新定位或已启动），返回true，否则返回false。
     */
    boolean active();

    /**
     * 检查分片是否已经被分配到一个特定的节点。
     * @return 如果分片已经被分配到节点，返回true，否则返回false。
     */
    boolean assignedToNode();

    /**
     * 返回当前分片所在的节点ID。
     * @return 当前节点ID的字符串。
     */
    String currentNodeId();

    /**
     * 如果分片正在重新定位，返回它将被移动到的节点ID。
     * @return 正在重新定位的节点ID的字符串，如果不在重新定位，则返回null。
     */
    String relocatingNodeId();

    /**
     * 检查分片是否为主分片。
     * @return 如果分片是主分片，返回true，如果是副本分片，则返回false。
     */
    boolean primary();

    /**
     * 返回分片路由的当前状态。
     * @return 分片路由状态枚举。
     */
    ShardRoutingState state();

    /**
     * 返回表示分片在集群中身份的ShardId对象。
     * @return ShardId对象。
     */
    ShardId shardId();

    /**
     * 提供分片路由的简洁摘要，不包括索引名称和分片ID。
     * @return 分片路由的字符串摘要。
     */
    String shortSummary();

    /**
     * 将分片路由信息序列化到DataOutput流中，不包括索引名称和分片ID。
     * 这是一种轻量级的序列化方法。
     * @param out 要写入的DataOutput流。
     * @throws IOException 如果写入过程中发生I/O错误。
     */
    void writeToThin(DataOutput out) throws IOException;

    /**
     * 从DataInput流中反序列化分片路由信息。
     * 此方法通常用于从使用writeToThin写入的流中读取。
     * @param in 要读取的DataInput流。
     * @throws ClassNotFoundException 如果反序列化的对象的类找不到。
     * @throws IOException 如果读取过程中发生I/O错误。
     */
    void readFromThin(DataInput in) throws ClassNotFoundException, IOException;
}
