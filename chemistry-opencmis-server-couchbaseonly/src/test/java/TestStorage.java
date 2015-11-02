//package org.apache.chemistry.opencmis.couchbase.test;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.math.BigInteger;

import org.apache.chemistry.opencmis.commons.data.ContentStream;
import org.apache.chemistry.opencmis.commons.impl.dataobjects.ContentStreamImpl;
import org.apache.chemistry.opencmis.couchbase.CouchbaseService;
import org.apache.chemistry.opencmis.couchbase.CouchbaseStorageService;
import org.apache.chemistry.opencmis.couchbase.StorageException;
import org.apache.chemistry.opencmis.couchbase.StorageService;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestStorage {

	static String storagePath = "/Users/cecilelepape/Documents/CMIS/repo/couchbase";
	static String tempPath = "/Users/cecilelepape/Documents/tools/apache-tomcat-8.0-2.24/temp";

	static final String repoId = "test";
	static CouchbaseService cbService = null;
	static StorageService storageService = null;

	public TestStorage() {
	}

	@BeforeClass
	static public void before() {
		// recupérer les propriétés de repository.properties
		storageService = new CouchbaseStorageService("cmiscouchbase");

	}

	@AfterClass
	static public void after() {

		if (storageService != null) {
			storageService.close();
		}
	}

	@Test
	public void testSmallStorage() {
		String content = "This is a test file called TestCouchbaseStorage.txt and stored in folderA";
		ContentStream cStream = new ContentStreamImpl("TestCouchbaseStorage.txt", "text/plain", content);
		try {
			String dataId = "TestCouchbaseStorage";
			storageService.writeContent(dataId, cStream);
			ContentStream result = storageService.getContent(dataId, BigInteger.valueOf(0), BigInteger.valueOf(0), "TestCouchbaseStorage.txt");
			assertNotNull("Result is null",result);
			assertTrue("The test file does not exist", storageService.exists(dataId));
			storageService.deleteContent(dataId);
			assertTrue("The test file should have been removed", !storageService.exists(dataId));			
		} catch (StorageException e) {
			e.printStackTrace();
			assertTrue("Storage error : "+e.getMessage(),Boolean.FALSE);
		}
	}

	@Test
	public void testLargeStorage() {
		String content = "This is a test file called TestCouchbaseStorage.txt and stored in folderA";
		ContentStream cStream = new ContentStreamImpl("TestCouchbaseStorage.txt", "text/plain", content);
		try {
			String dataId = "TestCouchbaseStorage";
			storageService.writeContent(dataId, cStream);
			ContentStream result = storageService.getContent(dataId, BigInteger.valueOf(0), BigInteger.valueOf(0), "TestCouchbaseStorage.txt");
			assertNotNull("Result is null",result);
			assertTrue("The test file does not exist", storageService.exists(dataId));
			storageService.deleteContent(dataId);
			assertTrue("The test file should have been removed", !storageService.exists(dataId));			
		} catch (StorageException e) {
			e.printStackTrace();
			assertTrue("Storage error : "+e.getMessage(),Boolean.FALSE);
		}
	}
}
