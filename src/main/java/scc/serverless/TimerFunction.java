package scc.serverless;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.microsoft.azure.functions.annotation.*;
import com.microsoft.azure.functions.*;
import redis.clients.jedis.Jedis;
import scc.cache.Cache;
import scc.data.channel.ChannelDAO;
import scc.data.channel.ChannelsDBLayer;
import scc.data.message.MessageDAO;
import scc.data.message.MessagesDBLayer;
import scc.data.user.UserDAO;
import scc.data.user.UsersDBLayer;

/**
 * Azure Functions with Timer Trigger.
 */
public class TimerFunction {
    @FunctionName("gc-channels")
    public void garbageCollectChannels( @TimerTrigger(name = "periodicSetTime", schedule = "10 */* * * * *") String timerInfo, ExecutionContext context) {
        for (ChannelDAO channelDAO : ChannelsDBLayer.getInstance().getDeletedChannels()) {
            MessagesDBLayer.getInstance().deleteChannelsMessages(channelDAO.getId());
            ChannelsDBLayer.getInstance().delChannelById(channelDAO.getId());
        }
    }

    @FunctionName("gc-users")
    public void garbageCollectUsers( @TimerTrigger(name = "periodicSetTime", schedule = "10 */* * * * *") String timerInfo, ExecutionContext context) {
        for (UserDAO userDAO : UsersDBLayer.getInstance().getDeletedUsers()) {
            for(MessageDAO msg : MessagesDBLayer.getInstance().getMsgsSentByUser(userDAO.getId())) {
                msg.setSend("NA");
                MessagesDBLayer.getInstance().updateMessage(msg);
            }
            for(MessageDAO msg : MessagesDBLayer.getInstance().getMsgsReceivedByUser(userDAO.getId())) {
                msg.setDest("NA");
                MessagesDBLayer.getInstance().updateMessage(msg);
            }
            UsersDBLayer.getInstance().delUserById(userDAO.getId());
        }
    }

    @FunctionName("updc_msg")
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
