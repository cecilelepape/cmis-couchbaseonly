//package org.apache.chemistry.opencmis.couchbase.test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import junit.framework.Assert;

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

public class TestReadOnlyUser {

	static final String bucketId = "beer-sample";

	private Cluster cluster = null;
	private Bucket bucket = null;

	public TestReadOnlyUser() {
	}

	@Before
	public void before() {
		cluster = CouchbaseCluster.create();
		
	}

	@After
	public void after() {
		cluster.disconnect();
	}

	@Test
	public void allTests() {
		testGet();
	}

	@SuppressWarnings("deprecation")
	private void testGet(){
		bucket = cluster.openBucket(bucketId, "Cecile");
		JsonDocument doc = bucket.get("21st_amendment_brewery_cafe");
		assertNotNull("Bucket cannot be read", doc);
		System.out.println("JSonDocument = "+doc);

		
		try{
			bucket.upsert(doc);
			System.out.println("ERREUR : doc has been updated by read only user");
			Assert.assertTrue("doc has been updated by read only user", false);
			System.out.println("Y'a un problème");
		}
		catch(Exception e){
			Assert.assertTrue("doc has not been updated by read only user", true);
			System.out.println("Tout va bien");
		}	
	}
}
