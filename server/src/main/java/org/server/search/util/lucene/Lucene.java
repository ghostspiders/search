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

package org.server.search.util.lucene;
import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.*;
import org.server.search.index.engine.Engine;
import org.server.search.util.gnu.trove.TIntArrayList;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


public class Lucene {

    /**
     * 使用StandardAnalyzer创建的分析器实例，用于处理文本的分词。
     * StandardAnalyzer是Lucene的标准分析器，适用于大多数文本分析场景。
     * 它是公开的（public）、静态的（static）、并且是最终的（final），意味着这个实例在初始化后不能被修改。
     */
    public static final StandardAnalyzer STANDARD_ANALYZER = new StandardAnalyzer();

    /**
     * 使用KeywordAnalyzer创建的分析器实例，用于处理不进行分词的文本。
     * KeywordAnalyzer通常用于索引那些需要精确匹配的字段，如关键字或ID。
     * 它是公开的（public）、静态的（static）、并且是最终的（final），确保这个实例在使用过程中保持不变。
     */
    public static final KeywordAnalyzer KEYWORD_ANALYZER = new KeywordAnalyzer();

    public static final int NO_DOC = -1;

    /**
     * 使用给定的查询和最小分数条件来计数匹配的文档数量。
     *
     * @param searcher Lucene的IndexSearcher对象，用于执行搜索操作。
     * @param query Lucene的Query对象，定义了搜索条件。
     * @param minScore 只有得分高于这个值的文档才会被计入总数。
     * @return 满足查询条件并且得分高于minScore的文档数量。
     * @throws IOException 如果搜索过程中发生I/O错误。
     */
    public static long count(IndexSearcher searcher, Query query, float minScore) throws IOException {
        // 创建一个CountCollector实例，用于收集得分高于minScore的文档
        CountCollector countCollector = new CountCollector(minScore);

        // 使用searcher执行查询，并将countCollector作为Collector传入
        searcher.search(query, countCollector);

        // 从countCollector获取计数结果并返回
        return countCollector.count();
    }

    /**
     * 根据给定的术语（Term）在索引中查找第一个匹配的文档ID。
     *
     * @param reader Lucene的IndexReader对象，用于读取索引数据。
     * @param term 要搜索的术语。
     * @return 如果找到匹配的文档，则返回该文档的ID；如果没有找到，则返回NO_DOC（-1）。
     * @throws IOException 如果读取索引时发生I/O错误。
     */
    public static int docId(IndexSearcher searcher, Term term) throws IOException {
        TermQuery termQuery = new TermQuery(term);
        TopDocs topDocs = searcher.search(termQuery, 10);
        ScoreDoc[] hits = topDocs.scoreDocs;
        if(hits.length >0){
            return hits[0].doc;
        }
        return NO_DOC;
    }
    /**
     * 安全关闭提供的IndexReader实例。
     * 如果IndexReader为null，则认为已经关闭，返回true。
     * 如果关闭过程中发生IOException，则返回false，表示关闭失败。
     *
     * @param reader 需要关闭的IndexReader实例。
     * @return 如果IndexReader成功关闭或为null，则返回true；否则返回false。
     */
    public static boolean safeClose(IndexReader reader) {
        if (reader == null) {
            return true; // 如果reader为null，认为它已经关闭
        }
        try {
            reader.close(); // 尝试关闭reader
            return true; // 关闭成功
        } catch (IOException e) {
            return false; // 捕获IOException，关闭失败
        }
    }

    /**
     * 安全关闭提供的IndexWriter实例。
     * 如果IndexWriter为null，则认为已经关闭，返回true。
     * 如果关闭过程中发生IOException，则返回false，表示关闭失败。
     *
     * @param writer 需要关闭的IndexWriter实例。
     * @return 如果IndexWriter成功关闭或为null，则返回true；否则返回false。
     */
    public static boolean safeClose(IndexWriter writer) {
        if (writer == null) {
            return true; // 如果writer为null，认为它已经关闭
        }
        try {
            writer.close(); // 尝试关闭writer
            return true; // 关闭成功
        } catch (IOException e) {
            return false; // 捕获IOException，关闭失败
        }
    }

    /**
     * 从DataInput流中读取TopDocs对象。
     *
     * @param in DataInput流，包含序列化的TopDocs数据。
     * @return 根据输入流中的数据构造的TopDocs对象。
     * @throws IOException 如果读取数据时发生I/O异常。
     */
    public static TopDocs readTopDocs(DataInput in) throws IOException {
        // 检查是否存在文档
        if (!in.readBoolean()) {
            // 没有文档，返回null
            return null;
        }

        // 读取是否包含排序字段的标志
        boolean hasSort = in.readBoolean();
        int totalHits = in.readInt();  // 读取总的命中数量
        float maxScore = in.readFloat();  // 读取最高得分

        SortField[] fields = null;
        FieldDoc[] fieldDocs = null;
        if (hasSort) {
            // 如果包含排序字段，则读取排序字段信息
            int fieldCount = in.readInt();
            fields = new SortField[fieldCount];
            for (int i = 0; i < fieldCount; i++) {
                String fieldname = in.readUTF();  // 读取字段名称
                int type = in.readInt();         // 读取字段类型
                boolean reverse = in.readBoolean();  // 读取是否逆序排序
                SortField.Type typeName = SortField.Type.values()[type];
                fields[i] = new SortField(fieldname, typeName, reverse);
            }

            // 读取排序字段的文档信息
            int docCount = in.readInt();
            fieldDocs = new FieldDoc[docCount];
            for (int i = 0; i < docCount; i++) {
                int fieldLength = in.readInt();  // 读取当前文档的排序字段数量
                Comparable[] cFields = new Comparable[fieldLength];
                for (int j = 0; j < fieldLength; j++) {
                    byte type = in.readByte();  // 读取字段值的类型
                    switch (type) {
                        case 0:
                            cFields[j] = in.readUTF();
                            break;
                        case 1:
                            cFields[j] = in.readInt();
                            break;
                        case 2:
                            cFields[j] = in.readLong();
                            break;
                        case 3:
                            cFields[j] = in.readFloat();
                            break;
                        case 4:
                            cFields[j] = in.readDouble();
                            break;
                        case 5:
                            cFields[j] = in.readByte();
                            break;
                        default:
                            throw new IOException("Can't match type [" + type + "]");
                    }
                }
                int docID = in.readInt();
                float score = in.readFloat();
                fieldDocs[i] = new FieldDoc(docID, score, cFields);
            }
            return new TopFieldDocs(new TotalHits(totalHits,TotalHits.Relation.EQUAL_TO), fieldDocs, fields);
        } else {
            // 如果不包含排序字段，则只读取得分和文档ID
            ScoreDoc[] scoreDocs = new ScoreDoc[totalHits];
            for (int i = 0; i < totalHits; i++) {
                int docID = in.readInt();
                float score = in.readFloat();
                scoreDocs[i] = new ScoreDoc(docID, score);
            }
            return new TopDocs(new TotalHits(totalHits,TotalHits.Relation.EQUAL_TO), scoreDocs);
        }
    }

    /**
     * 将TopDocs对象写入到DataOutput流中。
     *
     * @param out DataOutput流，用于写入TopDocs数据。
     * @param topDocs 要写入的TopDocs对象。
     * @param from 从哪个索引开始写入文档，用于分页或限制输出数量。
     * @throws IOException 如果写入数据时发生I/O异常。
     */
    public static void writeTopDocs(DataOutput out, TopDocs topDocs, int from) throws IOException {
        // 检查是否有文档可写入
        if (topDocs.scoreDocs.length - from < 0) {
            out.writeBoolean(false); // 没有可写入的文档
            return;
        }
        out.writeBoolean(true); // 标记有文档数据

        // 检查是否为TopFieldDocs类型，即是否包含排序字段
        if (topDocs instanceof TopFieldDocs) {
            out.writeBoolean(true); // 标记包含排序字段
            TopFieldDocs topFieldDocs = (TopFieldDocs) topDocs;

            // 写入总的命中数量和最高得分
            out.writeInt((int) topDocs.totalHits.value);
            ScoreDoc[] hits = topDocs.scoreDocs;
            if (hits.length > 0) {
                ScoreDoc topHit = hits[0]; // 得分最高的文档
                float maxScore = topHit.score; // 最高分数值
                out.writeFloat(maxScore);
            }


            // 写入排序字段信息
            out.writeInt(topFieldDocs.fields.length);
            for (SortField sortField : topFieldDocs.fields) {
                out.writeUTF(sortField.getField()); // 排序字段名称
                out.writeInt(sortField.getType().ordinal()); // 排序字段类型
                out.writeBoolean(sortField.getReverse()); // 是否逆序排序
            }

            // 写入文档信息，但不包括from之前的结果
            out.writeInt(topDocs.scoreDocs.length - from);
            int index = 0;
            for (ScoreDoc doc : topFieldDocs.scoreDocs) {
                if (index++ < from) {
                    continue; // 跳过from之前的结果
                }
                FieldDoc fieldDoc = (FieldDoc) doc;
                // 写入文档的排序字段值
                out.writeInt(fieldDoc.fields.length);
                for (Object field : fieldDoc.fields) {
                    // 根据字段值的类型写入不同的数据
                    Class type = field.getClass();
                    if (type == String.class) {
                        out.write(0);
                        out.writeUTF((String) field);
                    } else if (type == Integer.class) {
                        out.write(1);
                        out.writeInt((Integer) field);
                    } else if (type == Long.class) {
                        out.write(2);
                        out.writeLong((Long) field);
                    } else if (type == Float.class) {
                        out.write(3);
                        out.writeFloat((Float) field);
                    } else if (type == Double.class) {
                        out.write(4);
                        out.writeDouble((Double) field);
                    } else if (type == Byte.class) {
                        out.write(5);
                        out.write((Byte) field);
                    } else {
                        throw new IOException("Can't handle sort field value of type [" + type + "]");
                    }
                }

                // 写入文档ID和得分
                out.writeInt(doc.doc);
                out.writeFloat(doc.score);
            }
        } else {
            // 如果不是TopFieldDocs类型，则不包含排序字段
            out.writeBoolean(false); // 标记不包含排序字段
            // 写入总的命中数量和最高得分
            out.writeInt((int) topDocs.totalHits.value);
            ScoreDoc[] hits = topDocs.scoreDocs;
            if (hits.length > 0) {
                ScoreDoc topHit = hits[0]; // 得分最高的文档
                float maxScore = topHit.score; // 最高分数值
                out.writeFloat(maxScore);
            }

            // 写入文档信息，但不包括from之前的结果
            out.writeInt(topDocs.scoreDocs.length - from);
            int index = 0;
            for (ScoreDoc doc : topDocs.scoreDocs) {
                if (index++ < from) {
                    continue; // 跳过from之前的结果
                }
                // 写入文档ID和得分
                out.writeInt(doc.doc);
                out.writeFloat(doc.score);
            }
        }
    }

    public static Explanation readExplanation(DataInput in) throws IOException {
        float value = in.readFloat();
        String description = in.readUTF();
        Explanation explanation = Explanation.match(value, description);
        List<Explanation> list = new ArrayList<>();
        if (in.readBoolean()) {
            int size = in.readInt();
            for (int i = 0; i < size; i++) {
                list.add(Explanation.match(value, description));
            }
        }
        return Explanation.match(value, description,list);
    }

    public static void writeExplanation(DataOutput out, Explanation explanation) throws IOException {
        out.writeFloat(explanation.getValue().floatValue());
        out.writeUTF(explanation.getDescription());
        Explanation[] subExplanations = explanation.getDetails();
        if (subExplanations == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            out.writeInt(subExplanations.length);
            for (Explanation subExp : subExplanations) {
                writeExplanation(out, subExp);
            }
        }
    }

    /**
     * 自定义的Collector实现，用于计算得分高于指定最小值的文档数量。
     */
    public static class CountCollector implements Collector {
        // 得分的最小值，只有高于这个值的文档才会被计数
        private final float minScore;
        // 当前文档的得分，由Lucene在搜索时提供
        private  Scorable scorer;
        // 计数器，用于记录满足条件的文档数量
        private long count;

        /**
         * CountCollector构造函数。
         *
         * @param minScore 只有得分高于这个值的文档才会被计数。
         */
        public CountCollector(float minScore) {
            this.minScore = minScore;
        }

        /**
         * 获取当前计数的文档数量。
         *
         * @return 满足得分条件的文档数量。
         */
        public long count() {
            return this.count;
        }

        @Override
        public LeafCollector getLeafCollector(LeafReaderContext context) throws IOException {
            return new LeafCollector() {
                @Override
                public void setScorer(Scorable scorer1) throws IOException {
                    scorer = scorer1;
                }

                @Override
                public void collect(int doc) throws IOException {
                    if (scorer.score() > minScore) {
                        count++;
                    }
                }
            };
        }

        @Override
        public ScoreMode scoreMode() {
            return ScoreMode.COMPLETE;
        }
    }

    private Lucene() {

    }
}
