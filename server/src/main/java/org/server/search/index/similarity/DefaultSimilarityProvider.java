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

package org.server.search.index.similarity;

import com.google.inject.Inject;
import com.google.inject.assistedinject.Assisted;
import org.apache.lucene.search.similarities.ClassicSimilarity;
import org.server.search.index.Index;
import org.server.search.index.settings.IndexSettings;
import org.server.search.util.settings.Settings;

 
public class DefaultSimilarityProvider extends AbstractSimilarityProvider<ClassicSimilarity> {

    private ClassicSimilarity similarity;

    @Inject public DefaultSimilarityProvider(Index index, @IndexSettings Settings indexSettings, @Assisted String name, @Assisted Settings settings) {
        super(index, indexSettings, name);
        this.similarity = new ClassicSimilarity();
    }

    @Override public ClassicSimilarity get() {
        return similarity;
    }
}
