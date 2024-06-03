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

import com.google.inject.Inject;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.StoredFieldVisitor;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.*;
import org.server.search.SearchException;
import org.server.search.SearchIllegalArgumentException;
import org.server.search.SearchIllegalStateException;
import org.server.search.cluster.routing.ShardRouting;
import org.server.search.index.engine.Engine;
import org.server.search.index.engine.EngineException;
import org.server.search.index.engine.ScheduledRefreshableEngine;
import org.server.search.index.mapper.DocumentMapper;
import org.server.search.index.mapper.DocumentMapperNotFoundException;
import org.server.search.index.mapper.MapperService;
import org.server.search.index.mapper.ParsedDocument;
import org.server.search.index.query.IndexQueryParser;
import org.server.search.index.query.IndexQueryParserMissingException;
import org.server.search.index.query.IndexQueryParserService;
import org.server.search.index.settings.IndexSettings;
import org.server.search.index.store.Store;
import org.server.search.index.translog.Translog;
import org.server.search.threadpool.ThreadPool;
import org.server.search.util.Nullable;
import org.server.search.util.SizeValue;
import org.server.search.util.Strings;
import org.server.search.util.TimeValue;
import org.server.search.util.concurrent.ThreadSafe;
import org.server.search.util.lucene.Lucene;
import org.server.search.util.settings.Settings;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ScheduledFuture;


@IndexShardLifecycle
@ThreadSafe
public class InternalIndexShard extends AbstractIndexShardComponent implements IndexShard {
    // 线程池，用于执行索引分片的各种操作，如索引、搜索、刷新等
    private final ThreadPool threadPool;

    // 映射服务，管理索引的映射配置，包括字段类型、默认值等信息
    private final MapperService mapperService;

    // 查询解析服务，负责将用户的查询字符串转换为可执行的查询操作
    private final IndexQueryParserService queryParserService;

    // 存储服务，负责将索引数据持久化到磁盘，管理数据文件和元数据
    private final Store store;

    // 引擎，处理文档的索引、搜索和删除操作，是Elasticsearch的核心组件之一
    private final Engine engine;

    // 事务日志，用于记录所有变更操作，保证在故障恢复时数据不会丢失
    private final Translog translog;

    // 互斥锁，用于同步对IndexShard状态的修改，保证线程安全
    private final Object mutex = new Object();

    // 当前索引分片的状态，如初始、启动、停止等
    private volatile IndexShardState state;

    // 计划任务，用于安排分片的自动刷新操作
    private ScheduledFuture<?> refreshScheduledFuture;

    // 当前分片的路由信息，包括分片ID、是否为主分片、当前分配给哪个节点等
    private volatile ShardRouting shardRouting;

    @Inject public InternalIndexShard(ShardId shardId, @IndexSettings Settings indexSettings, Store store, Engine engine, Translog translog,
                                      ThreadPool threadPool, MapperService mapperService, IndexQueryParserService queryParserService) {
        super(shardId, indexSettings);
        this.store = store;
        this.engine = engine;
        this.translog = translog;
        this.threadPool = threadPool;
        this.mapperService = mapperService;
        this.queryParserService = queryParserService;
        state = IndexShardState.CREATED;
    }

    public Store store() {
        return this.store;
    }

    public Engine engine() {
        return engine;
    }

    public Translog translog() {
        return translog;
    }

    public ShardRouting routingEntry() {
        return this.shardRouting;
    }


    /**
     * 设置分片的路由入口。
     * 如果传入的ShardRouting的分片ID与当前分片的ID不匹配，则抛出SearchIllegalArgumentException异常。
     * 如果当前分片已经有路由入口，并且新的路由入口不是主分片而旧的是，记录一条警告日志。
     * 最后，更新当前分片的路由入口为传入的ShardRouting。
     */
    public InternalIndexShard routingEntry(ShardRouting shardRouting) {
        if (!shardRouting.shardId().equals(shardId())) {
            throw new SearchIllegalArgumentException("Trying to set a routing entry with shardId [" + shardRouting.shardId() + "] on a shard with shardId [" + shardId() + "]");
        }
        if (this.shardRouting != null) {
            if (!shardRouting.primary() && this.shardRouting.primary()) {
                logger.warn("Suspect illegal state: Trying to move shard from primary mode to backup mode");
            }
        }
        this.shardRouting = shardRouting;
        return this;
    }

    /**
     * 将分片状态设置为恢复中。
     * 首先，使用synchronized同步代码块以确保线程安全。
     * 如果分片状态为已关闭、已启动、已迁移或已恢复中，则抛出相应的异常。
     * 如果当前状态不是以上状态，则将分片状态设置为恢复中，并返回之前的状态。
     */
    public IndexShardState recovering() throws IndexShardStartedException, IndexShardRelocatedException, IndexShardRecoveringException, IndexShardClosedException {
        synchronized (mutex) {
            IndexShardState returnValue = state;
            if (state == IndexShardState.CLOSED) {
                throw new IndexShardClosedException(shardId);
            }
            if (state == IndexShardState.STARTED) {
                throw new IndexShardStartedException(shardId);
            }
            if (state == IndexShardState.RELOCATED) {
                throw new IndexShardRelocatedException(shardId);
            }
            if (state == IndexShardState.RECOVERING) {
                throw new IndexShardRecoveringException(shardId);
            }
            state = IndexShardState.RECOVERING;
            return returnValue;
        }
    }

    /**
     * 恢复分片的恢复状态。
     * 使用synchronized同步代码块以确保线程安全。
     * 如果当前分片不是恢复中状态，则抛出IndexShardNotRecoveringException异常。
     * 否则，将分片状态恢复为传入的状态。
     */
    public InternalIndexShard restoreRecoveryState(IndexShardState stateToRestore) {
        synchronized (mutex) {
            if (this.state != IndexShardState.RECOVERING) {
                throw new IndexShardNotRecoveringException(shardId, state);
            }
            this.state = stateToRestore;
        }
        return this;
    }

    /**
     * 标记分片为已迁移。
     * 使用synchronized同步代码块以确保线程安全。
     * 如果分片状态不是启动状态，则抛出IndexShardNotStartedException异常。
     * 否则，将分片状态设置为已迁移。
     */
    public InternalIndexShard relocated() throws IndexShardNotStartedException {
        synchronized (mutex) {
            if (state != IndexShardState.STARTED) {
                throw new IndexShardNotStartedException(shardId, state);
            }
            state = IndexShardState.RELOCATED;
        }
        return this;
    }

    /**
     * 启动分片。
     * 使用synchronized同步代码块以确保线程安全。
     * 如果分片状态为已关闭、已启动或已迁移，则抛出相应的异常。
     * 如果当前状态不是以上状态，启动引擎，安排刷新计划任务，并将分片状态设置为启动。
     */
    public InternalIndexShard start() throws IndexShardStartedException, IndexShardRelocatedException, IndexShardClosedException {
        synchronized (mutex) {
            if (state == IndexShardState.CLOSED) {
                throw new IndexShardClosedException(shardId);
            }
            if (state == IndexShardState.STARTED) {
                throw new IndexShardStartedException(shardId);
            }
            if (state == IndexShardState.RELOCATED) {
                throw new IndexShardRelocatedException(shardId);
            }
            engine.start();
            scheduleRefresherIfNeeded();
            state = IndexShardState.STARTED;
        }
        return this;
    }

    public IndexShardState state() {
        return state;
    }

    /**
     * 估算可以刷新到磁盘的内存中数据的大小。
     * 如果不可用，则返回null。
     * @return SizeValue 表示估算的大小
     * @throws SearchException 如果写入不允许时抛出异常
     */
    public SizeValue estimateFlushableMemorySize() throws SearchException {
        writeAllowed(); // 检查是否允许写入操作
        return engine.estimateFlushableMemorySize(); // 调用引擎的方法来估算内存大小
    }

    /**
     * 创建一个新文档。
     * @param type 文档类型
     * @param id 文档ID
     * @param source 文档数据源（JSON字符串）
     * @throws SearchException 如果写入不允许或发生其他搜索异常时抛出
     */
    public void create(String type, String id, String source) throws SearchException {
        writeAllowed(); // 检查是否允许写入操作
        innerCreate(type, id, source); // 调用内部方法执行创建操作
    }

    /**
     * 内部方法，用于创建文档。
     * @param type 文档类型
     * @param id 文档ID
     * @param source 文档数据源（JSON字符串）
     */
    private void innerCreate(String type, String id, String source) {
        DocumentMapper docMapper = mapperService.type(type); // 获取文档映射
        if (docMapper == null) {
            throw new DocumentMapperNotFoundException("No mapper found for type [" + type + "]");
        }
        ParsedDocument doc = docMapper.parse(type, id, source); // 解析文档
        if (logger.isTraceEnabled()) {
            logger.trace("Indexing {}", doc); // 记录跟踪日志
        }
        engine.create(new Engine.Create(doc.doc(), docMapper.mappers().indexAnalyzer(), docMapper.type(), doc.id(), doc.source())); // 调用引擎的创建方法
    }

    /**
     * 索引一个文档（更新或创建）。
     * @param type 文档类型
     * @param id 文档ID
     * @param source 文档数据源（JSON字符串）
     * @throws SearchException 如果写入不允许或发生其他搜索异常时抛出
     */
    public void index(String type, String id, String source) throws SearchException {
        writeAllowed(); // 检查是否允许写入操作
        innerIndex(type, id, source); // 调用内部方法执行索引操作
    }

    private void innerIndex(String type, String id, String source) {
        DocumentMapper docMapper = mapperService.type(type);
        if (docMapper == null) {
            throw new DocumentMapperNotFoundException("No mapper found for type [" + type + "]");
        }
        ParsedDocument doc = docMapper.parse(type, id, source);
        if (logger.isTraceEnabled()) {
            logger.trace("Indexing {}", doc);
        }
        engine.index(new Engine.Index(docMapper.uidMapper().term(doc.uid()), doc.doc(), docMapper.mappers().indexAnalyzer(), docMapper.type(), doc.id(), doc.source()));
    }

    /**
     * 根据文档类型和ID删除文档。
     * @param type 文档类型
     * @param id 文档ID
     */
    public void delete(String type, String id) {
        writeAllowed(); // 检查是否允许写入操作
        DocumentMapper docMapper = mapperService.type(type); // 获取文档映射
        if (docMapper == null) {
            throw new DocumentMapperNotFoundException("No mapper found for type [" + type + "]");
        }
        innerDelete(docMapper.uidMapper().term(type, id)); // 调用内部方法执行删除操作
    }

    public void delete(Term uid) {
        writeAllowed();
        innerDelete(uid);
    }

    private void innerDelete(Term uid) {
        if (logger.isTraceEnabled()) {
            logger.trace("Deleting [{}]", uid.text());
        }
        engine.delete(new Engine.Delete(uid));
    }

    /**
     * 根据查询删除文档。
     * @param querySource 查询源字符串
     * @param queryParserName 查询解析器名称
     * @param types 要查询的文档类型数组
     * @throws SearchException 如果写入不允许或发生其他搜索异常时抛出
     */
    public void deleteByQuery(String querySource, @Nullable String queryParserName, String... types) throws SearchException {
        writeAllowed(); // 检查是否允许写入操作
        if (types == null) {
            types = Strings.EMPTY_ARRAY; // 如果types为空，则赋值为空数组
        }
        innerDeleteByQuery(querySource, queryParserName, types); // 调用内部方法执行删除操作
    }

    private void innerDeleteByQuery(String querySource, String queryParserName, String... types) {
        IndexQueryParser queryParser = queryParserService.defaultIndexQueryParser();
        if (queryParserName != null) {
            queryParser = queryParserService.indexQueryParser(queryParserName);
            if (queryParser == null) {
                throw new IndexQueryParserMissingException(queryParserName);
            }
        }
        Query query = queryParser.parse(querySource);
        query = filterByTypesIfNeeded(query, types);

        if (logger.isTraceEnabled()) {
            logger.trace("Deleting By Query [{}]", query);
        }

        engine.delete(new Engine.DeleteByQuery(query, querySource, queryParserName, types));
    }

    /**
     * 根据文档类型和ID获取文档。
     * @param type 文档类型
     * @param id 文档ID
     * @return 字符串形式的文档源
     * @throws SearchException 如果读取不允许或发生其他搜索异常时抛出
     */
    public String get(String type, String id) throws SearchException {
        readAllowed(); // 检查是否允许读取操作
        DocumentMapper docMapper = mapperService.type(type); // 获取文档映射
        if (docMapper == null) {
            throw new DocumentMapperNotFoundException("No mapper found for type [" + type + "]");
        }
        Engine.Searcher searcher = engine.searcher(); // 获取引擎的搜索器
        try {
            int docId = Lucene.docId(searcher.reader(), docMapper.uidMapper().term(type, id)); // 根据类型和ID获取文档ID
            if (docId == Lucene.NO_DOC) {
                if (logger.isTraceEnabled()) {
                    logger.trace("Get for [{}#{}] returned no result", type, id); // 记录跟踪日志
                }
                return null; // 如果没有找到文档，则返回null
            }
            StoredFieldVisitor storedFieldVisitor = docMapper.sourceMapper().fieldSelector(); // 获取存储字段访问器
            Document doc = searcher.reader().document(docId); // 获取文档
            if (logger.isTraceEnabled()) {
                logger.trace("Get for [{}#{}] returned [{}]", new Object[]{type, id, doc}); // 记录跟踪日志
            }
            return docMapper.sourceMapper().value(doc); // 返回文档的源数据
        } catch (IOException e) {
            throw new SearchException("Failed to get type [" + type + "] and id [" + id + "]", e); // 抛出搜索异常
        } finally {
            searcher.release(); // 释放搜索器资源
        }
    }

    /**
     * 根据提供的查询和可选的查询解析器名称，计算匹配的文档数量。
     * @param minScore 只计算大于或等于此分数的文档
     * @param querySource 查询的JSON字符串
     * @param queryParserName 查询解析器的名称，如果为null，则使用默认解析器
     * @param types 要搜索的文档类型数组，可以为空
     * @return 匹配的文档数量
     * @throws SearchException 如果读取不允许或发生其他搜索异常时抛出
     */
    public long count(float minScore, String querySource, @Nullable String queryParserName, String... types) throws SearchException {
        readAllowed(); // 检查是否允许读取操作
        IndexQueryParser queryParser = queryParserService.defaultIndexQueryParser(); // 获取默认的查询解析器
        if (queryParserName != null) { // 如果提供了查询解析器名称
            queryParser = queryParserService.indexQueryParser(queryParserName); // 获取指定名称的查询解析器
            if (queryParser == null) { // 如果指定的查询解析器不存在
                throw new IndexQueryParserMissingException(queryParserName); // 抛出异常
            }
        }
        Query query = queryParser.parse(querySource); // 解析查询字符串为查询对象
        query = filterByTypesIfNeeded(query, types); // 根据文档类型过滤查询，如果需要

        Engine.Searcher searcher = engine.searcher(); // 获取引擎的搜索器
        try {
            long count = Lucene.count(searcher.searcher(), query, minScore); // 执行计数查询
            if (logger.isTraceEnabled()) { // 如果启用了跟踪日志
                logger.trace("Count of [{}] is [{}]", query, count); // 记录查询和计数结果
            }
            return count; // 返回计数结果
        } catch (IOException e) { // 如果发生IO异常
            throw new SearchException("Failed to count query [" + query + "]", e); // 抛出搜索异常
        } finally {
            searcher.release(); // 释放搜索器资源
        }
    }

    /**
     * 刷新索引分片，使最近索引或删除的文档对搜索请求可见。
     * @param waitForOperations 如果为true，则刷新操作将等待任何正在进行的操作完成。
     * @throws SearchException 如果写入不允许或发生其他搜索异常时抛出
     * @throws IOException 如果发生IO异常
     */
    public void refresh(boolean waitForOperations) throws SearchException, IOException {
        writeAllowed(); // 检查是否允许写入操作
        if (logger.isTraceEnabled()) { // 如果启用了跟踪日志
            logger.trace("Refresh, waitForOperations[{}]", waitForOperations); // 记录刷新操作和等待标志
        }
        engine.refresh(waitForOperations); // 调用引擎的刷新方法
    }

    /**
     * 将内存中的索引数据刷新到磁盘。
     * @throws SearchException 如果写入不允许或发生其他搜索异常时抛出
     */
    public void flush() throws SearchException {
        writeAllowed(); // 检查是否允许写入操作
        if (logger.isTraceEnabled()) { // 如果启用了跟踪日志
            logger.trace("Flush"); // 记录刷新操作
        }
        engine.flush(); // 调用引擎的刷新方法
    }

    /**
     * 对索引分片进行快照操作。
     * @param snapshotHandler 快照处理器
     * @throws EngineException 如果发生引擎异常
     */
    public void snapshot(Engine.SnapshotHandler snapshotHandler) throws EngineException {
        readAllowed(); // 检查是否允许读取操作
        engine.snapshot(snapshotHandler); // 调用引擎的快照方法
    }

    /**
     * 执行索引分片的恢复操作。
     * @param recoveryHandler 恢复处理器
     * @throws EngineException 如果发生引擎异常
     */
    public void recover(Engine.RecoveryHandler recoveryHandler) throws EngineException {
        writeAllowed(); // 检查是否允许写入操作
        engine.recover(recoveryHandler); // 调用引擎的恢复方法
    }

    /**
     * 获取用于搜索的引擎搜索器。
     * @return Engine.Searcher 引擎搜索器实例
     */
    public Engine.Searcher searcher() {
        readAllowed(); // 检查是否允许读取操作
        return engine.searcher(); // 返回引擎的搜索器
    }

    /**
     * 关闭索引分片。
     * 释放相关资源并标记分片为已关闭状态。
     */
    public void close() {
        synchronized (mutex) { // 同步代码块以确保线程安全
            if (state != IndexShardState.CLOSED) { // 如果分片尚未关闭
                if (refreshScheduledFuture != null) { // 如果存在刷新计划任务
                    refreshScheduledFuture.cancel(true); // 取消刷新计划任务
                    refreshScheduledFuture = null; // 清空引用
                }
            }
            state = IndexShardState.CLOSED; // 设置分片状态为已关闭
        }
    }

    /**
     * 执行基于一系列事务日志操作的恢复。
     * @param operations 事务日志操作的Iterable
     * @throws SearchException 如果发生搜索异常
     */
    public void performRecovery(Iterable<Translog.Operation> operations) throws SearchException {
        if (state != IndexShardState.RECOVERING) {
            throw new IndexShardNotRecoveringException(shardId, state); // 如果分片不是恢复中状态，则抛出异常
        }
        engine.start(); // 启动引擎
        applyTranslogOperations(operations); // 应用事务日志操作
        synchronized (mutex) {
            state = IndexShardState.STARTED; // 更新分片状态为启动
        }
        scheduleRefresherIfNeeded(); // 如果需要，安排刷新计划任务
    }

    /**
     * 执行基于事务日志快照的恢复。
     * @param snapshot 事务日志的快照
     * @param phase3 如果是恢复的第三阶段，则为true
     * @throws SearchException 如果发生搜索异常
     */
    public void performRecovery(Translog.Snapshot snapshot, boolean phase3) throws SearchException {
        if (state != IndexShardState.RECOVERING) {
            throw new IndexShardNotRecoveringException(shardId, state); // 如果分片不是恢复中状态，则抛出异常
        }
        if (!phase3) {
            // 如果不是第三阶段，启动引擎，但分片尚未启动
            engine.start();
        }
        applyTranslogOperations(snapshot); // 应用事务日志快照操作
        if (phase3) {
            synchronized (mutex) {
                state = IndexShardState.STARTED; // 如果是第三阶段，更新分片状态为启动
            }
            scheduleRefresherIfNeeded(); // 如果需要，安排刷新计划任务
        }
    }

    /**
     * 应用事务日志操作。
     * @param snapshot 事务日志操作的Iterable
     */
    private void applyTranslogOperations(Iterable<Translog.Operation> snapshot) {
        for (Translog.Operation operation : snapshot) { // 遍历事务日志操作
            switch (operation.opType()) { // 根据操作类型
                case CREATE: // 创建操作
                    Translog.Create create = (Translog.Create) operation;
                    innerCreate(create.type(), create.id(), create.source()); // 调用内部方法执行创建
                    break;
                case SAVE: // 保存操作（索引）
                    Translog.Index index = (Translog.Index) operation;
                    innerIndex(index.type(), index.id(), index.source()); // 调用内部方法执行索引
                    break;
                case DELETE: // 删除操作
                    Translog.Delete delete = (Translog.Delete) operation;
                    innerDelete(delete.uid()); // 调用内部方法执行删除
                    break;
                case DELETE_BY_QUERY: // 按查询删除操作
                    Translog.DeleteByQuery deleteByQuery = (Translog.DeleteByQuery) operation;
                    innerDeleteByQuery(deleteByQuery.source(), deleteByQuery.queryParserName(), deleteByQuery.types());
                    // 调用内部方法执行按查询删除
                    break;
                default:
                    throw new SearchIllegalStateException("No operation defined for [" + operation + "]"); // 如果未知操作类型，抛出异常
            }
        }
    }


    /**
     * 检查分片是否可以忽略恢复尝试。
     * 如果分片已经处于恢复中、启动、迁移或已关闭状态，则可以忽略恢复尝试。
     * @return 如果可以忽略恢复尝试，返回true；否则返回false。
     */
    public boolean ignoreRecoveryAttempt() {
        IndexShardState state = state(); // 进行一次易变读取以获取当前状态
        return state == IndexShardState.RECOVERING || state == IndexShardState.STARTED ||
                state == IndexShardState.RELOCATED || state == IndexShardState.CLOSED;
    }

    /**
     * 确保分片处于允许读取的状态。
     * 如果分片状态不是启动或迁移状态，则抛出IllegalIndexShardStateException异常。
     */
    public void readAllowed() throws IllegalIndexShardStateException {
        IndexShardState state = this.state; // 进行一次易变读取以获取当前状态
        if (state != IndexShardState.STARTED && state != IndexShardState.RELOCATED) {
            throw new IllegalIndexShardStateException(shardId, state, "Read operations only allowed when started/relocated");
        }
    }

    /**
     * 确保分片处于允许写入的状态。
     * 如果分片状态不是启动状态，则抛出IndexShardNotStartedException异常。
     */
    public void writeAllowed() throws IllegalIndexShardStateException {
        IndexShardState state = this.state; // 进行一次易变读取以获取当前状态
        if (state != IndexShardState.STARTED) {
            throw new IndexShardNotStartedException(shardId, state);
        }
    }

    /**
     * 如果需要，安排刷新计划任务。
     * 如果引擎是ScheduledRefreshableEngine的实例并且刷新间隔大于0，则安排刷新任务。
     */
    private void scheduleRefresherIfNeeded() {
        if (engine instanceof ScheduledRefreshableEngine) {
            TimeValue refreshInterval = ((ScheduledRefreshableEngine) engine).refreshInterval();
            if (refreshInterval.millis() > 0) {
                refreshScheduledFuture = threadPool.scheduleWithFixedDelay(new EngineRefresher(), refreshInterval);
                logger.debug("Scheduling refresher every {}", refreshInterval);
            }
        }
    }

    private Query filterByTypesIfNeeded(Query query, String[] types) {
//        if (types != null && types.length > 0) {
//            if (types.length == 1) {
//                String type = types[0];
//                DocumentMapper docMapper = mapperService.documentMapper(type);
//                if (docMapper == null) {
//                    throw new TypeMissingException(shardId.index(), type);
//                }
//                TermFilter termFilter = new TermFilter(docMapper.typeMapper().term(docMapper.type()));
//                query = new BooleanQuery();
//            } else {
//                BooleanFilter booleanFilter = new BooleanFilter();
//                for (String type : types) {
//                    DocumentMapper docMapper = mapperService.documentMapper(type);
//                    if (docMapper == null) {
//                        throw new TypeMissingException(shardId.index(), type);
//                    }
//                    Filter typeFilter = new TermFilter(docMapper.typeMapper().term(docMapper.type()));
//                    typeFilter = filterCache.cache(typeFilter);
//                    booleanFilter.add(new FilterClause(typeFilter, BooleanClause.Occur.SHOULD));
//                }
//                query = new FilteredQuery(query, booleanFilter);
//            }
//        }
        return query;
    }

    private class EngineRefresher implements Runnable {
        @Override public void run() {
            try {
                engine.refresh(false);
            } catch (Exception e) {
                logger.warn("Failed to perform scheduled engine refresh", e);
            }
        }
    }
}