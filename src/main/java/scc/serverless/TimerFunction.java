package scc.serverless;

import java.text.SimpleDateFormat;
import java.util.*;
import com.microsoft.azure.functions.annotation.*;
import redis.clients.jedis.Jedis;
import scc.cache.RedisCache;
import com.microsoft.azure.functions.*;

/**
 * Azure Functions with Timer Trigger.
 */
public class TimerFunction {
    @FunctionName("periodic-compute")
    public void cosmosFunction( @TimerTrigger(name = "periodicSetTime", schedule = "30 */* * * * *") String timerInfo, ExecutionContext context) {

    }
}
