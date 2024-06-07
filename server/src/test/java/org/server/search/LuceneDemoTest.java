package org.server.search;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.*;
import org.apache.lucene.index.*;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.IOUtils;
import org.server.search.util.UUID;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.Assert.assertEquals;

/**
 * @author gaoyvfeng
 * @ClassName LuceneDemoTest
 * @description: TODO
 * @datetime 2024年 06月 05日 09:42
 * @version: 1.0
 */
public class LuceneDemoTest {
    public static void main(String[] args) throws IOException, ParseException {
        Analyzer analyzer = new StandardAnalyzer();

        Path indexPath = Files.createTempDirectory("tempIndex");
        Directory directory = FSDirectory.open(indexPath);
        IndexWriterConfig config = new IndexWriterConfig(analyzer);
        IndexWriter iwriter = new IndexWriter(directory, config);
        Document doc = new Document();
        String text = UUID.randomUUID().toString();
        doc.add(new KeywordField("fieldname",text, Field.Store.YES));
        iwriter.addDocument(doc);
        iwriter.close();

        // Now search the index:
        DirectoryReader ireader = DirectoryReader.open(directory);
        IndexSearcher isearcher = new IndexSearcher(ireader);
        // Parse a simple query that searches for "text":
//        QueryParser parser = new QueryParser("fieldname", analyzer);
//        Query query = parser.parse("text");
        Term term = new Term("fieldname", text);
        TermQuery query = new TermQuery(term);
        ScoreDoc[] hits = isearcher.search(query, 10).scoreDocs;
        assertEquals(1, hits.length);
        // Iterate through the results:
        StoredFields storedFields = isearcher.storedFields();
        for (int i = 0; i < hits.length; i++) {
            Document hitDoc = storedFields.document(hits[i].doc);
            assertEquals("This is the text to be indexed.", hitDoc.get("fieldname"));
        }
        ireader.close();
        directory.close();
        IOUtils.rm(indexPath);
    }
}
