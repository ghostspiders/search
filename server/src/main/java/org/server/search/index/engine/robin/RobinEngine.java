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

package org.server.search.index.engine.robin;

import com.google.inject.Inject;
import org.apache.lucene.index.*;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.store.Directory;
import org.server.search.SearchException;
import org.server.search.index.analysis.AnalysisService;
import org.server.search.index.deletionpolicy.SnapshotDeletionPolicy;
import org.server.search.index.deletionpolicy.SnapshotIndexCommit;
import org.server.search.index.engine.*;
import org.server.search.index.merge.policy.MergePolicyProvider;
import org.server.search.index.merge.scheduler.MergeSchedulerProvider;
import org.server.search.index.settings.IndexSettings;
import org.server.search.index.shard.AbstractIndexShardComponent;
import org.server.search.index.shard.IndexShardLifecycle;
import org.server.search.index.shard.ShardId;
import org.server.search.index.similarity.SimilarityService;
import org.server.search.index.store.Store;
import org.server.search.index.translog.Translog;
import org.server.search.util.Preconditions;
import org.server.search.util.SizeUnit;
import org.server.search.util.SizeValue;
import org.server.search.util.TimeValue;
import org.server.search.util.concurrent.resource.AcquirableResource;
import org.server.search.util.lucene.IndexWriters;
import org.server.search.util.lucene.ReaderSearcherHolder;
import org.server.search.util.settings.Settings;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static org.server.search.util.TimeValue.*;
import static org.server.search.util.concurrent.resource.AcquirableResourceFactory.*;
import static org.server.search.util.lucene.Lucene.*;

/**
 * 
 */
@IndexShardLifecycle
public class RobinEngine extends AbstractIndexShardComponent implements Engine, ScheduledRefreshableEngine {

    private final SizeValue ramBufferSize;

    private final TimeValue refreshInterval;

    private final int termIndexInterval;

    private final ReadWriteLock rwl = new ReentrantReadWriteLock();

    private final AtomicBoolean refreshMutex = new AtomicBoolean();

    private final Store store;

    private final SnapshotDeletionPolicy deletionPolicy;

    private final Translog translog;

    private final MergePolicyProvider mergePolicyProvider;

    private final MergeSchedulerProvider mergeScheduler;

    private final AnalysisService analysisService;

    private final SimilarityService similarityService;

    private volatile IndexWriter indexWriter;

    private volatile AcquirableResource<ReaderSearcherHolder> nrtResource;

    private volatile boolean closed = false;

    // flag indicating if a dirty operation has occurred since the last refresh
    private volatile boolean dirty = false;

    private volatile int disableFlushCounter = 0;

    @Inject public RobinEngine(ShardId shardId, @IndexSettings Settings indexSettings, Store store, SnapshotDeletionPolicy deletionPolicy, Translog translog,
                               MergePolicyProvider mergePolicyProvider, MergeSchedulerProvider mergeScheduler,
                               AnalysisService analysisService, SimilarityService similarityService) throws EngineException {
        super(shardId, indexSettings);
        Preconditions.checkNotNull(store, "Store must be provided to the engine");
        Preconditions.checkNotNull(deletionPolicy, "Snapshot deletion policy must be provided to the engine");
        Preconditions.checkNotNull(translog, "Translog must be provided to the engine");

        this.ramBufferSize = componentSettings.getAsSize("ramBufferSize", new SizeValue(64, SizeUnit.MB));
        this.refreshInterval = componentSettings.getAsTime("refreshInterval", timeValueSeconds(1));
        this.termIndexInterval = componentSettings.getAsInt("termIndexInterval", 128);

        this.store = store;
        this.deletionPolicy = deletionPolicy;
        this.translog = translog;
        this.mergePolicyProvider = mergePolicyProvider;
        this.mergeScheduler = mergeScheduler;
        this.analysisService = analysisService;
        this.similarityService = similarityService;
    }

    @Override public void start() throws EngineException {
        if (indexWriter != null) {
            throw new EngineAlreadyStartedException(shardId);
        }
        if (logger.isDebugEnabled()) {
            logger.debug("Starting engine with ramBufferSize [" + ramBufferSize + "], refreshInterval [" + refreshInterval + "]");
        }
        IndexWriter indexWriter = null;
        try {
            IndexWriterConfig config = new IndexWriterConfig();
            config.setOpenMode(IndexWriterConfig.OpenMode.CREATE_OR_APPEND);
            config.setMergeScheduler(mergeScheduler.newMergeScheduler());
            config.setMergePolicy(mergePolicyProvider.newMergePolicy(indexWriter));
            config.setSimilarity(similarityService.defaultIndexSimilarity());
            config.setRAMBufferSizeMB(ramBufferSize.mbFrac());

            indexWriter = new IndexWriter(store.directory(),config);
        } catch (IOException e) {
            safeClose(indexWriter);
            throw new EngineCreationFailureException(shardId, "Failed to create engine", e);
        }
        this.indexWriter = indexWriter;

        try {
            IndexReader indexReader = DirectoryReader.open(store.directory());
            IndexSearcher indexSearcher = new IndexSearcher(indexReader);
            indexSearcher.setSimilarity(similarityService.defaultSearchSimilarity());
            this.nrtResource = newAcquirableResource(new ReaderSearcherHolder(indexReader, indexSearcher));
        } catch (IOException e) {
            try {
                indexWriter.rollback();
            } catch (IOException e1) {
                // ignore
            } finally {
                try {
                    indexWriter.close();
                } catch (IOException e1) {
                    // ignore
                }
            }
            throw new EngineCreationFailureException(shardId, "Failed to open reader on writer", e);
        }
    }

    @Override public TimeValue refreshInterval() {
        return refreshInterval;
    }

    @Override public void create(Create create) throws EngineException {
        rwl.readLock().lock();
        try {
            indexWriter.addDocument(create.doc());
            translog.add(new Translog.Create(create));
            dirty = true;
        } catch (IOException e) {
            throw new CreateFailedEngineException(shardId, create, e);
        } finally {
            rwl.readLock().unlock();
        }
    }

    @Override public void index(Index index) throws EngineException {
        rwl.readLock().lock();
        try {
            indexWriter.updateDocument(index.uid(), index.doc());
            translog.add(new Translog.Index(index));
            dirty = true;
        } catch (IOException e) {
            throw new IndexFailedEngineException(shardId, index, e);
        } finally {
            rwl.readLock().unlock();
        }
    }

    @Override public void delete(Delete delete) throws EngineException {
        rwl.readLock().lock();
        try {
            indexWriter.deleteDocuments(delete.uid());
            translog.add(new Translog.Delete(delete));
            dirty = true;
        } catch (IOException e) {
            throw new DeleteFailedEngineException(shardId, delete, e);
        } finally {
            rwl.readLock().unlock();
        }
    }

    @Override public void delete(DeleteByQuery delete) throws EngineException {
        rwl.readLock().lock();
        try {
            indexWriter.deleteDocuments(delete.query());
            translog.add(new Translog.DeleteByQuery(delete));
            dirty = true;
        } catch (IOException e) {
            throw new DeleteByQueryFailedEngineException(shardId, delete, e);
        } finally {
            rwl.readLock().unlock();
        }
    }

    @Override public Searcher searcher() throws EngineException {
        AcquirableResource<ReaderSearcherHolder> holder;
        for (; ;) {
            holder = this.nrtResource;
            if (holder.acquire()) {
                break;
            }
            Thread.yield();
        }
        return new RobinSearchResult(holder);
    }

    @Override public SizeValue estimateFlushableMemorySize() {
        rwl.readLock().lock();
        try {
            long bytes = IndexWriters.estimateRamSize(indexWriter);
            bytes += translog.estimateMemorySize().bytes();
            return new SizeValue(bytes);
        } catch (Exception e) {
            return null;
        } finally {
            rwl.readLock().unlock();
        }
    }

    @Override public void refresh(boolean waitForOperations) throws EngineException {
        if (refreshMutex.compareAndSet(false, true)) {
            if (dirty) {
                dirty = false;
                AcquirableResource<ReaderSearcherHolder> current = nrtResource;
                IndexReader newReader = current.resource().reader();
                if (newReader != current.resource().reader()) {
                    nrtResource = newAcquirableResource(new ReaderSearcherHolder(newReader));
                    current.markForClose();
                }
            }
            refreshMutex.set(false);
        }
    }

    @Override public void flush() throws EngineException {
        // check outside the lock as well so we can check without blocking on the write lock
        if (disableFlushCounter > 0) {
            throw new FlushNotAllowedEngineException(shardId, "Recovery is in progress, flush is not allowed");
        }
        rwl.writeLock().lock();
        try {
            if (disableFlushCounter > 0) {
                throw new FlushNotAllowedEngineException(shardId, "Recovery is in progress, flush is not allowed");
            }
            try {
                indexWriter.commit();
                translog.newTranslog();
            } catch (IOException e) {
                throw new FlushFailedEngineException(shardId, e);
            }
        } finally {
            rwl.writeLock().unlock();
        }
    }

    @Override public void snapshot(SnapshotHandler snapshotHandler) throws EngineException {
        SnapshotIndexCommit snapshotIndexCommit = null;
        Translog.Snapshot traslogSnapshot = null;
        rwl.readLock().lock();
        try {
            snapshotIndexCommit = deletionPolicy.snapshot();
            traslogSnapshot = translog.snapshot();
        } catch (Exception e) {
            if (snapshotIndexCommit != null) snapshotIndexCommit.release();
            throw new SnapshotFailedEngineException(shardId, e);
        } finally {
            rwl.readLock().unlock();
        }

        try {
            snapshotHandler.snapshot(snapshotIndexCommit, traslogSnapshot);
        } finally {
            snapshotIndexCommit.release();
            traslogSnapshot.release();
        }
    }

    @Override public void recover(RecoveryHandler recoveryHandler) throws EngineException {
        // take a write lock here so it won't happen while a flush is in progress
        // this means that next commits will not be allowed once the lock is released
        rwl.writeLock().lock();
        try {
            disableFlushCounter++;
        } finally {
            rwl.writeLock().unlock();
        }

        SnapshotIndexCommit phase1Snapshot;
        try {
            phase1Snapshot = deletionPolicy.snapshot();
        } catch (IOException e) {
            --disableFlushCounter;
            throw new RecoveryEngineException(shardId, 1, "Snapshot failed", e);
        }

        try {
            recoveryHandler.phase1(phase1Snapshot);
        } catch (Exception e) {
            --disableFlushCounter;
            phase1Snapshot.release();
            throw new RecoveryEngineException(shardId, 1, "Execution failed", e);
        }

        Translog.Snapshot phase2Snapshot;
        try {
            phase2Snapshot = translog.snapshot();
        } catch (Exception e) {
            --disableFlushCounter;
            phase1Snapshot.release();
            throw new RecoveryEngineException(shardId, 2, "Snapshot failed", e);
        }

        try {
            recoveryHandler.phase2(phase2Snapshot);
        } catch (Exception e) {
            --disableFlushCounter;
            phase1Snapshot.release();
            phase2Snapshot.release();
            throw new RecoveryEngineException(shardId, 2, "Execution failed", e);
        }

        rwl.writeLock().lock();
        Translog.Snapshot phase3Snapshot;
        try {
            phase3Snapshot = translog.snapshot(phase2Snapshot);
        } catch (Exception e) {
            --disableFlushCounter;
            rwl.writeLock().unlock();
            phase1Snapshot.release();
            phase2Snapshot.release();
            throw new RecoveryEngineException(shardId, 3, "Snapshot failed", e);
        }

        try {
            recoveryHandler.phase3(phase3Snapshot);
        } catch (Exception e) {
            throw new RecoveryEngineException(shardId, 3, "Execution failed", e);
        } finally {
            --disableFlushCounter;
            rwl.writeLock().unlock();
            phase1Snapshot.release();
            phase2Snapshot.release();
            phase3Snapshot.release();
        }
    }

    @Override public void close() throws SearchException {
        if (closed) {
            return;
        }
        closed = true;
        rwl.writeLock().lock();
        if (nrtResource != null) {
            this.nrtResource.forceClose();
        }
        try {
            if (indexWriter != null) {
                indexWriter.close();
            }
        } catch (IOException e) {
            throw new CloseEngineException(shardId, "Failed to close engine", e);
        } finally {
            indexWriter = null;
            rwl.writeLock().unlock();
        }
    }

    private static class RobinSearchResult implements Searcher {

        private final AcquirableResource<ReaderSearcherHolder> nrtHolder;

        private RobinSearchResult(AcquirableResource<ReaderSearcherHolder> nrtHolder) {
            this.nrtHolder = nrtHolder;
        }

        @Override public IndexReader reader() {
            return nrtHolder.resource().reader();
        }

        @Override public IndexSearcher searcher() {
            return nrtHolder.resource().searcher();
        }

        @Override public boolean release() throws SearchException {
            nrtHolder.release();
            return true;
        }
    }
}
