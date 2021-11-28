package scc.data.media;

import com.azure.core.util.BinaryData;
import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobContainerClientBuilder;
import scc.mgt.AzureProperties;

import javax.ws.rs.NotFoundException;

public class MediaBlobLayer {

	private static MediaBlobLayer instance;

	public static MediaBlobLayer getInstance() {
		if(instance == null) {
			BlobContainerClient containerClient = new BlobContainerClientBuilder()
					.connectionString(AzureProperties.getProperty( "BlobStoreConnection"))
					.containerName("images")
					.buildClient();
			instance = new MediaBlobLayer(containerClient);
		}

		return instance;
	}

	private final BlobContainerClient containerClient;

	private MediaBlobLayer(BlobContainerClient containerClient) {
		this.containerClient = containerClient;
	}
	
	public void upload(String id, byte[] contents) {
		BlobClient blob = containerClient.getBlobClient(id);
		blob.upload(BinaryData.fromBytes(contents));
	}
	
	public byte[] download(String id) {
		BlobClient result = containerClient.getBlobClient(id);
		if(!result.exists())
			throw new NotFoundException();

		return result.downloadContent().toBytes();	
	}

	public void delete(String id) {
		BlobClient result = containerClient.getBlobClient(id);
		result.delete();
	}
}
