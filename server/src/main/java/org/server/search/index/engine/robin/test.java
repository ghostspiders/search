if (indexWriter != null) {
        throw new EngineAlreadyStartedException(shardId);
        }
        if (logger.isDebugEnabled()) {
        logger.debug("Starting engine with ramBufferSize [" + ramBufferSize + "], refreshInterval [" + refreshInterval + "]");
        }
        Analyzer analyzer = new StandardAnalyzer();
        IndexWriter indexWriter = null;
        try {
        IndexWriterConfig config = new IndexWriterConfig();
        config.setOpenMode(IndexWriterConfig.OpenMode.CREATE_OR_APPEND);
        config.setMergeScheduler(mergeScheduler.newMergeScheduler());
        config.setMergePolicy(mergePolicyProvider.newMergePolicy(indexWriter));
        config.setSimilarity(similarityService.defaultIndexSimilarity());
        config.setRAMBufferSizeMB(ramBufferSize.mbFrac());
        indexWriter = new IndexWriter(store.directory(),config);
        indexWriter.commit();
        } catch (IOException e) {
        safeClose(indexWriter);
        throw new EngineCreationFailureException(shardId, "Failed to create engine", e);
        }
//        this.indexWriter = indexWriter;
        try {
        Document doc = new Document();
        String text = "This is the text to be indexed.";
        doc.add(new Field("fieldname", text, TextField.TYPE_STORED));
        indexWriter.addDocument(doc);
        indexWriter.commit();

        DirectoryReader indexReader = DirectoryReader.open(store.directory());
        IndexSearcher indexSearcher = new IndexSearcher(indexReader);
        indexSearcher.setSimilarity(similarityService.defaultSearchSimilarity());
        this.nrtResource = newAcquirableResource(new ReaderSearcherHolder(indexReader, indexSearcher));
        // Now search the index:

        // Parse a simple query that searches for "text":
        QueryParser parser = new QueryParser("fieldname", analyzer);
        Query query = parser.parse("text");
        ScoreDoc[] hits = searcher() .searcher().search(query, 10).scoreDocs;
        assertEquals(1, hits.length);
        // Iterate through the results:
        StoredFields storedFields = searcher() .searcher().storedFields();
        for (int i = 0; i < hits.length; i++) {
        Document hitDoc = storedFields.document(hits[i].doc);
        System.out.println(hitDoc.get("fieldname"));
        assertEquals("This is the text to be indexed.", hitDoc.get("fieldname"));
        }













