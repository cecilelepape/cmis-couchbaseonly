package org.apache.chemistry.opencmis.couchbase;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigInteger;

import org.apache.chemistry.opencmis.commons.data.ContentStream;
import org.apache.chemistry.opencmis.commons.impl.dataobjects.ContentStreamImpl;
import org.apache.chemistry.opencmis.commons.impl.dataobjects.PartialContentStreamImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.couchbase.client.deps.io.netty.buffer.ByteBuf;
import com.couchbase.client.deps.io.netty.buffer.Unpooled;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.document.BinaryDocument;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.json.JsonObject;

public class CouchbaseStorageService implements StorageService {

	private static final Logger LOG = LoggerFactory
			.getLogger(CouchbaseStorageService.class);

	private static final int BUFFER_SIZE = 1048576;
	private static final String STORAGE_ID = "local";
	private static final String PART_SUFFIX = "::part";
	
	private Cluster cluster = null;
	private Bucket bucket = null;
	private String bucketId = null;

	public CouchbaseStorageService(String bucketId) {
		this.bucketId = bucketId;
		cluster = CouchbaseCluster.create();
		bucket = cluster.openBucket(this.bucketId);
		debug("CouchbaseService started : bucket=" + bucket.name());
	}

	public void close() {
	}

	/**
	 * ContentStream is split into parts of 1MB
	 */
	public void writeContent(String dataId, ContentStream contentStream)
			throws StorageException {
		debug("writeContent dataId:" + dataId);

		// count the number of parts
		long length = contentStream.getLength();
		System.out
				.println("length=" + length + " - buffer size=" + BUFFER_SIZE);
		long nbparts = length / BUFFER_SIZE;
		// the last part
		if (length - nbparts * BUFFER_SIZE > 0)
			nbparts++;
		
		System.out.println("nbparts=" + nbparts);

		JsonObject doc = JsonObject.empty();
		doc.put("count", nbparts);
		doc.put("mimetype", contentStream.getMimeType());
		doc.put("length", length);

		long totalLength = 0;
		int read = 0; // The number of bytes not yet read
		byte[] byteArray = new byte[BUFFER_SIZE];
		int offset = 0;
		for (int i = 0; i < nbparts; i++) {
			try {
				read = contentStream.getStream()
						.read(byteArray, 0, BUFFER_SIZE);
				System.out.println("wrote " + read + " bytes beginning from "
						+ offset);

				totalLength += read;
				System.out.println("Number of bytes read : " + totalLength);
				offset += read;
				writeContentPart(dataId + PART_SUFFIX + i, byteArray, read);
				doc.put(dataId + PART_SUFFIX + i, read);
			} catch (IOException e) {
				e.printStackTrace();
				System.out
						.println("Pb with reading stream : " + e.getMessage());
			}
		}

		System.out.println("Number of bytes read : " + totalLength
				+ " -  length=" + length);
		if (totalLength != length)
			throw new StorageException("Wrong number of bytes read from stream");
		

		JsonDocument jsondoc = JsonDocument.create(dataId, doc);
		bucket.upsert(jsondoc);

	}

	private void writeContentPart(String partId, byte[] bytesArray, int length)
			throws StorageException {
		BinaryDocument bDoc = BinaryDocument.create(partId,
				Unpooled.copiedBuffer(bytesArray));
		bucket.upsert(bDoc);
	}

	/**
	 * Delete a content. Since folder are not materialized, only document are
	 * deleted from the storage system.
	 * 
	 * @param dataId
	 *            the content identifier.
	 * @return
	 */
	public boolean deleteContent(String dataId) {
		System.out.println("deleteContent dataId="+dataId);
		JsonDocument doc = bucket.get(dataId);
		JsonObject json = doc.content();
		
		// delete the main doc
		bucket.remove(dataId);
		
		// delete each part
		Integer nbparts = json.getInt("count");
		if(nbparts==null) return true;
		for(int i=0 ; i<nbparts ; i++){
			bucket.remove(dataId + PART_SUFFIX + i);
		}
		return true;
	}

	public org.apache.chemistry.opencmis.commons.data.ContentStream getContent(
			String dataId, BigInteger offset, BigInteger length, String filename)
			throws StorageException {
		debug("getContent dataId="+dataId+" filename="+filename+" - length="+length);
		StringBuffer mimeType = new StringBuffer();
		InputStream stream = getInputStream(dataId, mimeType);

		// compile data
		ContentStreamImpl result;
		if ((offset != null && offset.longValue() > 0) || length != null) {
			result = new PartialContentStreamImpl();
		} else {
			result = new ContentStreamImpl();
		}

		result.setFileName(filename);
		result.setLength(length);
		result.setMimeType(mimeType.toString());
		result.setStream(stream);

		return result;
	}

	private InputStream getInputStream(String dataId, StringBuffer mimeType)
			throws StorageException {
		JsonDocument doc = bucket.get(dataId);
		JsonObject json = doc.content();
		Integer nbparts = json.getInt("count");
		Integer length = json.getInt("length");
		
		if(nbparts==null || length==null || mimeType==null) throw new StorageException("Document invalid : nbparts, lenght and mimetype are mandatory");

		mimeType.append(json.getString("mimetype"));
		
		System.out.println("nb of parts = " + nbparts);
		System.out.println("length = " + length);
		System.out.println("mimetype=" + json.getString("mimetype"));
		
		byte[] byteArray = new byte[length];
		// for each part, read the content into the byteArray
		int offset = 0;
		Integer partLength = null;
		
		for (int i = 0; i < nbparts; i++) {
			System.out.println("reading part " + i);
			partLength = json.getInt(dataId + PART_SUFFIX + i);
			if(partLength == null) throw new StorageException("length of part "+i+" is mandatory");
			System.out.println("nb of bytes to read = " + partLength);
			BinaryDocument bDoc = bucket.get(dataId + PART_SUFFIX + i,
					BinaryDocument.class);
			ByteBuf part = bDoc.content();

			byte[] dst = new byte[partLength];
			part.readBytes(dst);
			for (int k = 0; k < partLength; k++) {
				byteArray[k + offset] = dst[k];
			}
			offset += partLength;
			part.release();
		}

		InputStream stream = new ByteArrayInputStream(byteArray);
		
		return stream;
	}

	public boolean exists(String dataId) {
		return bucket.get(dataId) != null;
	}

	private void debug(String msg) {
		if (LOG.isDebugEnabled()) {
			LOG.debug("{}", msg);
		}
	}

	@Override
	public String getStorageId() {
		return STORAGE_ID;
	}
}
