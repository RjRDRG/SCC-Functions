package scc.serverless;

import com.microsoft.azure.functions.annotation.*;
import com.microsoft.azure.functions.*;
import data.channel.ChannelDAO;
import data.channel.ChannelsDBLayer;
import data.message.MessageDAO;
import data.message.MessagesDBLayer;
import data.user.UserDAO;
import data.user.UsersDBLayer;

/**
 * Azure Functions with Timer Trigger.
 */
public class TimerFunction {

    private boolean started = false;
    private static MessagesDBLayer messagesDBLayer;
    private static ChannelsDBLayer channelsDBLayer;
    private static UsersDBLayer usersDBLayer;

    private void start() {
        if(!started) {
            messagesDBLayer = new MessagesDBLayer();
            channelsDBLayer = new ChannelsDBLayer();
            usersDBLayer = new UsersDBLayer();
            started = true;
        }
    }

    @FunctionName("gc-channels")
    public void garbageCollectChannels( @TimerTrigger(name = "periodicSetTime", schedule = "0 */1 * * * *") String timerInfo, ExecutionContext context) {
        start();
        for (ChannelDAO channelDAO : channelsDBLayer.getDeletedChannels()) {
            messagesDBLayer.deleteChannelsMessages(channelDAO.getId());
            channelsDBLayer.delChannelById(channelDAO.getId());
        }
    }

    @FunctionName("gc-users")
    public void garbageCollectUsers( @TimerTrigger(name = "periodicSetTime", schedule = "0 */1 * * * *") String timerInfo, ExecutionContext context) {
        start();
        for (UserDAO userDAO : usersDBLayer.getDeletedUsers()) {
            for(MessageDAO msg : messagesDBLayer.getMsgsSentByUser(userDAO.getId())) {
                msg.setUser("NA");
                messagesDBLayer.updateMessage(msg);
            }
            usersDBLayer.delUserById(userDAO.getId());
        }
    }
}
