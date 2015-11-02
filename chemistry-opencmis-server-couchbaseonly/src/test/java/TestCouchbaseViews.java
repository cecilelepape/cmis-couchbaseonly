//package org.apache.chemistry.opencmis.couchbase.test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.util.Arrays;
import java.util.List;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.bucket.BucketManager;
import com.couchbase.client.java.view.DefaultView;
import com.couchbase.client.java.view.DesignDocument;
import com.couchbase.client.java.view.View;
import com.couchbase.client.java.view.ViewQuery;
import com.couchbase.client.java.view.ViewResult;
import com.couchbase.client.java.view.ViewRow;
import com.couchbase.client.*;

public class TestCouchbaseViews {

	static final String bucketId = "test";
	static final String view = "docsbymodifdate";
	private BucketManager manager;


	private Cluster cluster = null;
	private Bucket bucket = null;
	
	public TestCouchbaseViews() {
	}

	
	@Before
	 public void before(){
		cluster = CouchbaseCluster.create();
		bucket = cluster.openBucket(bucketId);
		
		manager = bucket.bucketManager();
	}
	
	@After
	 public void after(){
		cluster.disconnect();
	}

	@Test
	public void testQueryView() {

		ViewResult result = bucket.query(ViewQuery.from("dev_bymodificationdate", view));

		// Iterate through the returned ViewRows
		for (ViewRow row : result) {
		    System.out.println(row);
		}
		
	}
	
	@Test
    public void testInsertDesignDocument() {
		String map1 = "function (doc, meta) {if(doc['cmis:lastModificationDate']){ emit(doc['cmis:lastModificationDate'], meta.id);}";
        List<View> views = Arrays.asList(DefaultView.create("v1", map1, "_count"));
        DesignDocument designDocument = DesignDocument.create("insert1", views);
        manager.insertDesignDocument(designDocument);

        DesignDocument found = manager.getDesignDocument("insert1");
        assertNotNull(found);
        assertEquals("insert1", found.name());
        assertEquals(1, found.views().size());
        assertEquals("function(d,m){}", found.views().get(0).map());
        assertEquals("_count", found.views().get(0).reduce());
    }

}
