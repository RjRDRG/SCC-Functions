package scc.serverless;

import com.microsoft.azure.functions.annotation.*;

import redis.clients.jedis.Jedis;
import scc.cache.Cache;
import scc.data.message.MessageDAO;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.microsoft.azure.functions.*;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

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
            List<MessageDAO> messageDAOS = Arrays.stream(msgs).map(s ->{  //TODO test if message at 0 is newest message
                try {
                    return mapper.readValue(s,MessageDAO.class);
                } catch (JsonProcessingException e) {
                    e.printStackTrace();
                    throw new RuntimeException(e);
                }
            }).sorted(Comparator.comparing(m -> LocalDateTime.ofInstant(Instant.ofEpochMilli((long) (Double.parseDouble(m.get_ts()) * 1000)), ZoneId.systemDefault())))
                    .collect(Collectors.toList());
            for (MessageDAO msg : messageDAOS) {
                Long cnt = jedis.lpush("MostRecentMsgs:" + msg.getChannel(), mapper.writeValueAsString(msg));
                if (cnt > 20)
                    jedis.ltrim("MostRecentMsgs:" + msg.getChannel(), 0, 19);
            }
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
    }

}