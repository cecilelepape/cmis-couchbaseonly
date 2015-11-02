//package org.apache.chemistry.opencmis.couchbase.test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.chemistry.opencmis.commons.impl.json.JSONObject;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.bucket.BucketManager;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.view.DefaultView;
import com.couchbase.client.java.view.DesignDocument;
import com.couchbase.client.java.view.View;
import com.couchbase.client.java.view.ViewQuery;
import com.couchbase.client.java.view.ViewResult;
import com.couchbase.client.java.view.ViewRow;
import com.couchbase.client.*;

public class TestCouchbaseViewsVsLookup {

	static final String bucketId = "beer-sample";
	static final String view = "breweryByName";
	static final String designdoc = "dev_beer";

	private Cluster cluster = null;
	private Bucket bucket = null;

	public TestCouchbaseViewsVsLookup() {
	}

	@Before
	public void before() {
		cluster = CouchbaseCluster.create();
		bucket = cluster.openBucket(bucketId);
	}

	@After
	public void after() {
		cluster.disconnect();
	}

	@Test
	public void allTests() {
		testViewQuery();
		testBuildNameIndex();
		testCompareLookupAndView();
	}

	public void testViewQuery() {
		long start = System.currentTimeMillis();
		ViewResult result = bucket.query(ViewQuery.from(designdoc, view));

		int i = 0;
		String id;
		for (ViewRow row : result) {
			// System.out.println(row.id());
			id = row.id();
			i++;
		}
		// System.out.println("\n======\n");
		System.out.println("Query all docs with views in "
				+ (System.currentTimeMillis() - start) + "ms");
		// System.out.println("Number of results = "+results.size());
		System.out.println("Number of results = " + i);

	}

	public void testBuildNameIndex() {
		long start = System.currentTimeMillis();
		ViewResult result = bucket.query(ViewQuery.from(designdoc, view));

		JsonDocument lookupDoc = null;
		JsonObject lookupContent = null;

		int i = 0;
		for (ViewRow row : result) {
			i++;
			// row.key() = name of the brewery
			// row.value() = id of the brewery doc
			// creation of lookup docs
			lookupContent = JsonObject.empty();
			lookupContent.put("key", row.value());
			lookupDoc = JsonDocument.create((String) row.key(), lookupContent);
			bucket.upsert(lookupDoc);
		}
		System.out.println("Number of lookup docs created = " + i + " in "
				+ (System.currentTimeMillis() - start) + "ms");
	}

	public void testCompareLookupAndView() {
		String breweryName = "8-Ball Stout";
		long start1 = System.currentTimeMillis();
		String idByView = doTestView(breweryName);
		long end1 = System.currentTimeMillis();
		System.out.println("id by view = " + idByView + " retrieved in "
				+ (end1 - start1) + "ms");
		long start2 = System.currentTimeMillis();
		String idByLookup = doTestLookup(breweryName);
		long end2 = System.currentTimeMillis();
		System.out.println("id by lookup = " + idByLookup + " retrieved in "
				+ (end2 - start2) + "ms");

	}

	private String doTestView(String name) {
		// collection of ViewRow with field = name of brewery, value = id of
		// brewery
		ViewResult result = bucket.query(ViewQuery.from(designdoc, view).key(
				name));
		// retrieve the first result
		JsonObject doc = result.rows().next().document().content();
		System.out.println("[VIEW] the brewery named \"" + name + "\" is :"
				+ doc);
		return doc.getString("name");
	}

	private String doTestLookup(String name) {
		// unique doc with key = name of brewery, value = id of brewery
		JsonDocument index = bucket.get(name);
		// retrieve the value of field "key"
		String docId = index.content().getString("key");
		// retrieve the doc with key
		JsonDocument doc = bucket.get(docId);
		System.out.println("[LOOKUP] the brewery named \"" + name + "\" is :"
				+ doc);
		return docId;
	}

}
