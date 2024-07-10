package com.searchscale.solr;

import java.io.IOException;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest.Create;
import org.apache.solr.client.solrj.request.CollectionAdminRequest.Delete;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrInputDocument;

public class ParallelIndexingQuerying {
	public static void main( String[] args ) throws SolrServerException, IOException, InterruptedException {

		int queryThreads = Integer.parseInt(args[0]);
		int indexThreads = Integer.parseInt(args[1]);
		int numDocs = Integer.parseInt(args[2]);
		int numShards = Integer.parseInt(args[3]);
		String solrUrl = args[4];
		
		final HttpSolrClient solr = new HttpSolrClient.Builder(solrUrl).build();

		try {
			System.out.println(Delete.deleteCollection("test").process(solr).getResponse());
		} catch (Exception ex) {
			ex.printStackTrace();
		}
		System.out.println(Create.createCollection("test", numShards, 1).process(solr).getResponse());

		ExecutorService indexingPool = Executors.newFixedThreadPool(indexThreads);
		ExecutorService queryingPool = Executors.newFixedThreadPool(queryThreads);

		AtomicInteger docsIndexed = new AtomicInteger(0);
		
		System.out.println("Starting indexing...");
		doIndexing(numDocs, 1000, "test", solr, indexingPool, docsIndexed);
		System.out.println("Starting querying...");
		doQuerying(numDocs, "test", solr, queryingPool, docsIndexed);

		indexingPool.shutdown();
		queryingPool.shutdown();
		indexingPool.awaitTermination(Integer.MAX_VALUE, TimeUnit.SECONDS);
		queryingPool.awaitTermination(Integer.MAX_VALUE, TimeUnit.SECONDS);
		solr.close();
	}

	private static void doIndexing(final int numDocs, final int commitInterval, final String collection,
			final HttpSolrClient solr, ExecutorService indexingPool, final AtomicInteger docsIndexed) {
		for (int i=0; i<numDocs; i++) {
			final int index = i;
			indexingPool.submit(new Runnable() {
				public void run() {
					SolrInputDocument doc = new SolrInputDocument("id", ""+index);
					if (index % commitInterval == 0) {
						System.out.println("INDEXING: " + index + ": " + doc);
						try {
							solr.commit(collection);
						} catch (SolrServerException | IOException e) {
							e.printStackTrace();
						}
					}
					try {
						solr.add(collection, doc);
						docsIndexed.incrementAndGet();
					} catch (SolrServerException | IOException  e) {
						e.printStackTrace();
					}
				}
			});
		}
	}
	private static void doQuerying(final int numQueries, final String collection,
			final HttpSolrClient solr, ExecutorService indexingPool, final AtomicInteger docsIndexed) {
		for (int i=0; i<numQueries; i++) {
			final int index = i;
			indexingPool.submit(new Runnable() {
				public void run() {
					try {
						SolrQuery query = new SolrQuery("id:" + new Random().nextInt(docsIndexed.get()));
						QueryResponse response = solr.query(collection, query);
						if (index % 1000 == 0) System.out.println("QUERYING: " + index + ": " + response.getResponseHeader());;
					} catch (SolrServerException | IOException e) {
						e.printStackTrace();
					}
				}
			});
		}
	}

}
