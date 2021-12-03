package scc.serverless;

import com.microsoft.azure.functions.annotation.*;

import com.microsoft.azure.functions.*;
import data.media.MediaBlobLayer;

/**
 * Azure Functions with Blob Trigger.
 */
public class BlobStoreFunction {

    private static boolean started = false;
    private static MediaBlobLayer mediaBlobLayer;

    private void start() {
        if(!started) {
            mediaBlobLayer = new MediaBlobLayer(true);
            started = true;
        }
    }

    @FunctionName("replicate")
    public void replicateImageBlobs(
            @BlobTrigger(name = "blobtest", dataType = "binary", path = "images/{name}", connection = "BlobStoreConnection") byte[] content,
            @BindingName("name") String blobname, final ExecutionContext context) {
        start();
        mediaBlobLayer.upload(blobname, content);
    }
}