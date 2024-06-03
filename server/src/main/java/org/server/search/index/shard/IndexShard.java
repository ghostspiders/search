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

package org.server.search.index.shard;

import org.apache.lucene.index.Term;
import org.server.search.SearchException;
import org.server.search.cluster.routing.ShardRouting;
import org.server.search.index.engine.Engine;
import org.server.search.index.engine.EngineException;
import org.server.search.util.Nullable;
import org.server.search.util.SizeValue;
import org.server.search.util.concurrent.ThreadSafe;

import java.io.IOException;

 
@IndexShardLifecycle
@ThreadSafe
public interface IndexShard extends IndexShardComponent {
    // 返回当前分片的路由入口信息
    ShardRouting routingEntry();

    // 返回当前分片的状态
    IndexShardState state();

    /**
     * 返回估计的可刷新到磁盘的内存中数据的大小。
     * 如果该信息不可用，则返回null。
     */
    SizeValue estimateFlushableMemorySize() throws SearchException;

    // 创建一个新文档，指定类型、ID和文档源数据
    void create(String type, String id, String source) throws SearchException;

    // 索引一个文档，指定类型、ID和文档源数据
    void index(String type, String id, String source) throws SearchException;

    // 根据类型和ID删除文档
    void delete(String type, String id);

    // 根据Term（可能是类型和ID的组合）删除文档
    void delete(Term uid);

    // 根据查询源和文档类型执行删除操作
    void deleteByQuery(String querySource, @Nullable String queryParserName, String... types) throws SearchException;

    // 根据类型和ID获取文档源数据
    String get(String type, String id) throws SearchException;

    // 根据查询源、最小分数、查询解析器名称和文档类型执行计数操作
    long count(float minScore, String querySource, @Nullable String queryParserName, String... types) throws SearchException;

    // 刷新分片，可选地等待所有索引操作完成
    void refresh(boolean waitForOperations) throws SearchException, IOException;

    // 将内存中的数据刷新到磁盘
    void flush() throws SearchException;

    // 对分片进行快照操作
    void snapshot(Engine.SnapshotHandler snapshotHandler) throws EngineException;

    // 恢复分片
    void recover(Engine.RecoveryHandler recoveryHandler) throws EngineException;

    // 获取用于搜索的搜索引擎对象
    Engine.Searcher searcher();

    // 关闭分片
    void close();

    /**
     * 如果分片可以忽略恢复尝试（因为已经在恢复或已完成），则返回true。
     */
    public boolean ignoreRecoveryAttempt();
}
