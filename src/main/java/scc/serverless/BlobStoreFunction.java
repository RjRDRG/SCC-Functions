package scc.serverless;

import com.microsoft.azure.functions.annotation.*;

import com.microsoft.azure.functions.*;
import data.media.MediaBlobLayer;

/**
 * Azure Functions with Blob Trigger.
 */
public class BlobStoreFunction {

    private boolean started = false;
    private MediaBlobLayer mediaBlobLayer;

    private void start() {
        if(!started) {
            mediaBlobLayer = new MediaBlobLayer();
            started = true;
        }
    }

    @FunctionName("replicate")
    public void replicateBlobs(
            @BlobTrigger(name = "blobtest", dataType = "binary", path = "images/{name}", connection = "BlobStoreConnection") byte[] content,
            @BindingName("name") String blobname, final ExecutionContext context) {
        start();
        mediaBlobLayer.upload(blobname, content);
    }
}