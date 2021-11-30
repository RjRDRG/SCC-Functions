package scc.serverless;

import com.microsoft.azure.functions.annotation.*;

import scc.data.media.MediaBlobLayer;

import com.microsoft.azure.functions.*;

/**
 * Azure Functions with Blob Trigger.
 */
public class BlobStoreFunction {
    @FunctionName("replicate")
    public void replicateBlobs(
            @BlobTrigger(name = "blobtest", dataType = "binary", path = "images/{name}", connection = "BlobStoreConnection") byte[] content,
            @BindingName("name") String blobname, final ExecutionContext context) {
        MediaBlobLayer.getInstance().upload(blobname, content);
    }
}