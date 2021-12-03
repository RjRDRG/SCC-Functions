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

    public static final String USER = "user:";
    public static final String CHANNEL = "channel:";
    private static final String RECENT_MSGS = "mostRecentMsgs:";

    @FunctionName("updC_msg")
    public void updateCacheMsg(
            @CosmosDBTrigger(name = "cosmosTest", databaseName = "scc2122db", collectionName = "Messages", createLeaseCollectionIfNotExists = true, connectionStringSetting = "AzureCosmosDBConnection") String[] msgs,
            final ExecutionContext context) {
        if(!Boolean.parseBoolean(Optional.ofNullable(System.getenv("ENABLE_CACHE")).orElse("true")))
            return;

        try (Jedis jedis = Cache.getInstance().getResource()) {
            for (String msg : msgs) {
                String msgChannel = msg.split("channel\":\"")[1].split("\"")[0];
                Long cnt = jedis.lpush(RECENT_MSGS + msgChannel, msg);
                if (cnt > 20)
                    jedis.ltrim(RECENT_MSGS + msgChannel, 0, 19);
            }
        }
    }

    @FunctionName("updC_channel")
    public void updateCacheChannels(
            @CosmosDBTrigger(name = "cosmosTest", databaseName = "scc2122db", collectionName = "Channels", createLeaseCollectionIfNotExists = true, connectionStringSetting = "AzureCosmosDBConnection") String[] channels,
            final ExecutionContext context) {
        if(!Boolean.parseBoolean(Optional.ofNullable(System.getenv("ENABLE_CACHE")).orElse("true")))
            return;

        try (Jedis jedis = Cache.getInstance().getResource()) {
            for (String channel : channels) {
                String channelId = channel.split("\"id\":\"")[1].split("\"")[0];
                jedis.set(CHANNEL + channelId, channel);
            }
        }
    }

    @FunctionName("updC_user")
    public void updateCacheUsers(
            @CosmosDBTrigger(name = "cosmosTest", databaseName = "scc2122db", collectionName = "Users", createLeaseCollectionIfNotExists = true, connectionStringSetting = "AzureCosmosDBConnection") String[] users,
            final ExecutionContext context) {
        if(!Boolean.parseBoolean(Optional.ofNullable(System.getenv("ENABLE_CACHE")).orElse("true")))
            return;

        try (Jedis jedis = Cache.getInstance().getResource()) {
            for (String user : users) {
                String userId = user.split("\"id\":\"")[1].split("\"")[0];
                jedis.set(USER + userId, user);
            }
        }
    }

}