import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.StoredFields;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.IOUtils;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.jgroups.util.Util.assertEquals;

/**
 * @author gaoyvfeng
 * @ClassName LuceneTest
 * @description: TODO
 * @datetime 2024年 05月 13日 14:17
 * @version: 1.0
 * \
 *
 * /Users/gaoyvfeng/develop/code/IdeaProjects/search/work/search/indices/test
 */
public class LuceneCreateIndexTest {
    public static void main(String[] args) throws IOException, ParseException {
        Analyzer analyzer = new StandardAnalyzer();

//        Path indexPath = Paths.get("/Users/gaoyvfeng/develop/code/IdeaProjects/search/work/search/indices/test");
//        Directory directory = FSDirectory.open(indexPath);
        ByteBuffersDirectory directory = new ByteBuffersDirectory();
        IndexWriterConfig config = new IndexWriterConfig(analyzer);
        IndexWriter iwriter = new IndexWriter(directory, config);
        Document doc = new Document();
        String text = "This is the text to be indexed.";
        doc.add(new Field("fieldname", text, TextField.TYPE_STORED));
        iwriter.addDocument(doc);
        iwriter.close();

        // Now search the index:
        DirectoryReader ireader = DirectoryReader.open(directory);
        IndexSearcher isearcher = new IndexSearcher(ireader);
        // Parse a simple query that searches for "text":
        QueryParser parser = new QueryParser("fieldname", analyzer);
        Query query = parser.parse("text");
        ScoreDoc[] hits = isearcher.search(query, 10).scoreDocs;
        assertEquals(1, hits.length);
        // Iterate through the results:
        StoredFields storedFields = isearcher.storedFields();
        for (int i = 0; i < hits.length; i++) {
            Document hitDoc = storedFields.document(hits[i].doc);
            System.out.println(hitDoc.get("fieldname"));
            assertEquals("This is the text to be indexed.", hitDoc.get("fieldname"));
        }
        ireader.close();
        directory.close();
//        IOUtils.rm(indexPath);
    }
}
