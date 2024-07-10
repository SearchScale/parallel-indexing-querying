# parallel-indexing-querying
A simple Solr query and indxing load generator

    # Parameters: <indexing threads> <querying threads> <total docs> <numShards> <solr url>
    java -jar target/parallel-indexing-querying-1.0-SNAPSHOT-jar-with-dependencies.jar 4 4 20000 4 http://localhost:8983/solr

