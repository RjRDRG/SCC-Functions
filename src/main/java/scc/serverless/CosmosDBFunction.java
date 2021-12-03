package scc.serverless;

import cache.Cache;
import com.microsoft.azure.functions.annotation.*;
import redis.clients.jedis.Jedis;
import com.microsoft.azure.functions.*;
import java.util.Optional;

/**
 * Azure Functions with Timer Trigger.
 */
public class CosmosDBFunction {

    @FunctionName("updC_msg")
    public void updateCacheMsg(
            @CosmosDBTrigger(name = "cosmosTest", databaseName = "scc2122db", collectionName = "Messages", createLeaseCollectionIfNotExists = true, connectionStringSetting = "AzureCosmosDBConnection") String[] msgs,
            final ExecutionContext context) {
        if(!Boolean.parseBoolean(Optional.ofNullable(System.getenv("ENABLE_CACHE")).orElse("true")))
            return;

        try (Jedis jedis = Cache.getInstance().getResource()) {
            for (String msg : msgs) {
                String msgChannel = msg.split("channel\":\"")[1].split("\"")[0];
                Long cnt = jedis.lpush("MostRecentMsgs:" + msgChannel, msg);
                if (cnt > 20)
                    jedis.ltrim("MostRecentMsgs:" + msgChannel, 0, 19);
            }
        }
    }

}