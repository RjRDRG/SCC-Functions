package data.media;

import com.azure.core.util.BinaryData;
import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobContainerClientBuilder;

import javax.ws.rs.WebApplicationException;

public class MediaBlobLayer {

	private final BlobContainerClient containerClient;

	public MediaBlobLayer() {
		this.containerClient = new BlobContainerClientBuilder()
				.connectionString(System.getenv("BlobStoreConnection"))
				.containerName("images")
				.buildClient();
	}

	public void upload(String id, byte[] contents) {
		BlobClient blob = containerClient.getBlobClient(id);
		if(!blob.exists())
			blob.upload(BinaryData.fromBytes(contents));
	}
	
	public byte[] download(String id) {
		BlobClient result = containerClient.getBlobClient(id);
		if(!result.exists())
			throw new WebApplicationException(404);

		return result.downloadContent().toBytes();	
	}

	public void delete(String id) {
		BlobClient result = containerClient.getBlobClient(id);
		result.delete();
	}
}
