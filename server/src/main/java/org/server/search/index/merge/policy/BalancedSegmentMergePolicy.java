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

package org.server.search.index.merge.policy;

import org.apache.lucene.index.*;

import java.io.IOException;
import java.util.Set;

/**
 * Merge policy that tries to balance not doing large
 * segment merges with not accumulating too many segments in
 * the index, to provide for better performance in near
 * real-time setting.
 * <p/>
 * <p>This is based on code from zoie, described in more detail
 * at http://code.google.com/p/zoie/wiki/ZoieMergePolicy.</p>
 * <p/>
 * <p>See: https://issues.apache.org/jira/browse/LUCENE-1924</p>
 */
// TODO monitor against Lucene 3.0 trunk, once we move to 3.0 remove this.
public class BalancedSegmentMergePolicy extends LogByteSizeMergePolicy {
    public static final int DEFAULT_NUM_LARGE_SEGMENTS = 10;

    private boolean _partialExpunge = false;
    private int _numLargeSegments = DEFAULT_NUM_LARGE_SEGMENTS;
    private int _maxSmallSegments = 2 * LogMergePolicy.DEFAULT_MERGE_FACTOR;
    private int _maxSegments = _numLargeSegments + _maxSmallSegments;

    public BalancedSegmentMergePolicy(IndexWriter writer) {
        super();
    }

    public void setMergePolicyParams(MergePolicyParams params) {
        if (params != null) {
            setPartialExpunge(params._doPartialExpunge);
            setNumLargeSegments(params._numLargeSegments);
            setMaxSmallSegments(params._maxSmallSegments);
            setPartialExpunge(params._doPartialExpunge);
            setMergeFactor(params._mergeFactor);
            setMaxMergeDocs(params._maxMergeDocs);
        }
    }

    public void setPartialExpunge(boolean doPartialExpunge) {
        _partialExpunge = doPartialExpunge;
    }

    public boolean getPartialExpunge() {
        return _partialExpunge;
    }

    public void setNumLargeSegments(int numLargeSegments) {
        if (numLargeSegments < 2) {
            throw new IllegalArgumentException("numLargeSegments cannot be less than 2");
        }

        _numLargeSegments = numLargeSegments;
        _maxSegments = _numLargeSegments + 2 * getMergeFactor();
    }

    public int getNumLargeSegments() {
        return _numLargeSegments;
    }

    public void setMaxSmallSegments(int maxSmallSegments) {
        if (maxSmallSegments < getMergeFactor()) {
            throw new IllegalArgumentException("maxSmallSegments cannot be less than mergeFactor");
        }
        _maxSmallSegments = maxSmallSegments;
        _maxSegments = _numLargeSegments + _maxSmallSegments;
    }

    public int getMaxSmallSegments() {
        return _maxSmallSegments;
    }

    @Override
    public void setMergeFactor(int mergeFactor) {
        super.setMergeFactor(mergeFactor);
        if (_maxSmallSegments < getMergeFactor()) {
            _maxSmallSegments = getMergeFactor();
            _maxSegments = _numLargeSegments + _maxSmallSegments;
        }
    }

    private boolean isOptimized(SegmentInfos infos, IndexWriter writer, int maxNumSegments, Set<SegmentInfo> segmentsToOptimize) throws IOException {
        final int numSegments = infos.size();
        int numToOptimize = 0;
        SegmentInfo optimizeInfo = null;
        for (int i = 0; i < numSegments && numToOptimize <= maxNumSegments; i++) {
            final SegmentInfo info = infos.info(i).info;
            if (segmentsToOptimize.contains(info)) {
                numToOptimize++;
                optimizeInfo = info;
            }
        }

        return numToOptimize <= maxNumSegments &&
                (numToOptimize != 1 || isOptimized(writer, optimizeInfo));
    }

    private boolean isOptimized(IndexWriter writer, SegmentInfo info){
        return info.dir == writer.getDirectory();
    }

    private MergeSpecification findBalancedMerges(SegmentInfos infos, int infoLen, int maxNumSegments, boolean partialExpunge)
            throws IOException {
        if (infoLen <= maxNumSegments) return null;

        MergeSpecification spec = new MergeSpecification();
        // use Viterbi algorithm to find the best segmentation.
        // we will try to minimize the size variance of resulting segments.

        double[][] variance = createVarianceTable(infos, infoLen, maxNumSegments);

        final int maxMergeSegments = infoLen - maxNumSegments + 1;
        double[] sumVariance = new double[maxMergeSegments];
        int[][] backLink = new int[maxNumSegments][maxMergeSegments];

        for (int i = (maxMergeSegments - 1); i >= 0; i--) {
            sumVariance[i] = variance[0][i];
            backLink[0][i] = 0;
        }
        for (int i = 1; i < maxNumSegments; i++) {
            for (int j = (maxMergeSegments - 1); j >= 0; j--) {
                double minV = Double.MAX_VALUE;
                int minK = 0;
                for (int k = j; k >= 0; k--) {
                    double v = sumVariance[k] + variance[i + k][j - k];
                    if (v < minV) {
                        minV = v;
                        minK = k;
                    }
                }
                sumVariance[j] = minV;
                backLink[i][j] = minK;
            }
        }

        // now, trace back the back links to find all merges,
        // also find a candidate for partial expunge if requested
        int mergeEnd = infoLen;
        int prev = maxMergeSegments - 1;
        int expungeCandidate = -1;
        int maxDelCount = 0;
        for (int i = maxNumSegments - 1; i >= 0; i--) {
            prev = backLink[i][prev];
            int mergeStart = i + prev;
            if ((mergeEnd - mergeStart) > 1) {
                spec.add(new OneMerge(infos.asList()));
            } else {
                if (partialExpunge) {
                    SegmentInfo info = infos.info(mergeStart).info;
                    int delCount = 0;
                    if (delCount > maxDelCount) {
                        expungeCandidate = mergeStart;
                        maxDelCount = delCount;
                    }
                }
            }
            mergeEnd = mergeStart;
        }

        if (partialExpunge && maxDelCount > 0) {
            // expunge deletes
            spec.add(new OneMerge(infos.asList()));
        }

        return spec;
    }

    private double[][] createVarianceTable(SegmentInfos infos, int last, int maxNumSegments) throws IOException {
        int maxMergeSegments = last - maxNumSegments + 1;
        double[][] variance = new double[last][maxMergeSegments];

        // compute the optimal segment size
        long optSize = 0;
        long[] sizeArr = new long[last];
        for (int i = 0; i < sizeArr.length; i++) {
            sizeArr[i] = size(infos.info(i),null);
            optSize += sizeArr[i];
        }
        optSize = (optSize / maxNumSegments);

        for (int i = 0; i < last; i++) {
            long size = 0;
            for (int j = 0; j < maxMergeSegments; j++) {
                if ((i + j) < last) {
                    size += sizeArr[i + j];
                    double residual = ((double) size / (double) optSize) - 1.0d;
                    variance[i][j] = residual * residual;
                } else {
                    variance[i][j] = Double.NaN;
                }
            }
        }
        return variance;
    }
    public static class MergePolicyParams {
        private int _numLargeSegments;
        private int _maxSmallSegments;
        private boolean _doPartialExpunge;
        private int _mergeFactor;
        private boolean _useCompoundFile;
        private int _maxMergeDocs;

        public MergePolicyParams() {
            _useCompoundFile = true;
            _doPartialExpunge = false;
            _numLargeSegments = DEFAULT_NUM_LARGE_SEGMENTS;
            _maxSmallSegments = 2 * LogMergePolicy.DEFAULT_MERGE_FACTOR;
            _maxSmallSegments = _numLargeSegments + _maxSmallSegments;
            _mergeFactor = LogMergePolicy.DEFAULT_MERGE_FACTOR;
            _maxMergeDocs = LogMergePolicy.DEFAULT_MAX_MERGE_DOCS;
        }

        public void setNumLargeSegments(int numLargeSegments) {
            _numLargeSegments = numLargeSegments;
        }

        public int getNumLargeSegments() {
            return _numLargeSegments;
        }

        public void setMaxSmallSegments(int maxSmallSegments) {
            _maxSmallSegments = maxSmallSegments;
        }

        public int getMaxSmallSegments() {
            return _maxSmallSegments;
        }

        public void setPartialExpunge(boolean doPartialExpunge) {
            _doPartialExpunge = doPartialExpunge;
        }

        public boolean getPartialExpunge() {
            return _doPartialExpunge;
        }

        public void setMergeFactor(int mergeFactor) {
            _mergeFactor = mergeFactor;
        }

        public int getMergeFactor() {
            return _mergeFactor;
        }

        public void setMaxMergeDocs(int maxMergeDocs) {
            _maxMergeDocs = maxMergeDocs;
        }

        public int getMaxMergeDocs() {
            return _maxMergeDocs;
        }

        public void setUseCompoundFile(boolean useCompoundFile) {
            _useCompoundFile = useCompoundFile;
        }

        public boolean isUseCompoundFile() {
            return _useCompoundFile;
        }
    }
}
