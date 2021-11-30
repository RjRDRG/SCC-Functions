package scc.serverless;

import com.microsoft.azure.functions.annotation.*;

import redis.clients.jedis.Jedis;
import scc.cache.Cache;
import scc.data.message.MessageDAO;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.microsoft.azure.functions.*;

/**
 * Azure Functions with Timer Trigger.
 */
public class CosmosDBFunction {

    @FunctionName("updC_msg")
    public void updateCacheMsg(
            @CosmosDBTrigger(name = "cosmosTest", databaseName = "scc2122db", collectionName = "Messages", createLeaseCollectionIfNotExists = true, connectionStringSetting = "AzureCosmosDBConnection") String[] msgs,
            final ExecutionContext context) {
        try (Jedis jedis = Cache.getInstance().getResource()) {
            ObjectMapper mapper = new ObjectMapper();
            for (String m : msgs) {
                MessageDAO msg = mapper.readValue(m, MessageDAO.class);
                Long cnt = jedis.lpush("MostRecentMsgs:" + msg.getChannel(), m);
                if (cnt > 20)
                    jedis.ltrim("MostRecentMsgs:" + msg.getChannel(), 0, 19);
            }
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
    }

}