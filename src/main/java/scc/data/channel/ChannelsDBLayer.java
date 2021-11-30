package scc.data.channel;

import com.azure.cosmos.*;
import com.azure.cosmos.models.CosmosItemRequestOptions;
import com.azure.cosmos.models.CosmosQueryRequestOptions;
import com.azure.cosmos.models.PartitionKey;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import redis.clients.jedis.JedisPool;
import scc.cache.Cache;
import scc.mgt.AzureProperties;

import javax.ws.rs.BadRequestException;
import javax.ws.rs.NotFoundException;
import java.util.List;
import java.util.stream.Collectors;

public class ChannelsDBLayer {
    private static final String DB_NAME = "scc2122db";
	public static final String CHANNEL = "channel:";

	private static ChannelsDBLayer instance;

	public static synchronized ChannelsDBLayer getInstance() {
		if (instance == null) {
			CosmosClient client = new CosmosClientBuilder()
					.endpoint(AzureProperties.getProperty("COSMOSDB_URL"))
					.key(AzureProperties.getProperty("COSMOSDB_KEY"))
					.gatewayMode() // replace by .directMode() for better performance
					.consistencyLevel(ConsistencyLevel.SESSION).connectionSharingAcrossClientsEnabled(true)
					.contentResponseOnWriteEnabled(true).buildClient();
			JedisPool cache = Cache.getInstance();
			instance = new ChannelsDBLayer(client,cache);
		}

		return instance;
	}

	private final CosmosClient client;
	private final JedisPool cache;

	private CosmosContainer channels;

	public ChannelsDBLayer(CosmosClient client, JedisPool cache) {
		this.client = client;
		this.cache = cache;
	}

	private synchronized void init() {
		if (channels != null) return;
		CosmosDatabase db = client.getDatabase(DB_NAME);
		channels = db.getContainer("Channels");
	}

	public void delChannelById(String id) {
		init();
		if(cache!=null) {
			cache.getResource().del(CHANNEL + id);
		}
		if(channels.deleteItem(id, new PartitionKey(id), new CosmosItemRequestOptions()).getStatusCode() >= 400)
			throw new BadRequestException();
	}

	public void discardChannelById(String id) {
		init();
		ChannelDAO channelDAO = getChannelById(id);
		channelDAO.setGarbage(true);
		updateChannel(channelDAO);
	}

	public void createChannel(ChannelDAO channel) {
		init();
		if(cache!=null) {
			try {
				cache.getResource().set(CHANNEL + channel.getId(), new ObjectMapper().writeValueAsString(channel));
			} catch (JsonProcessingException e) {
				e.printStackTrace();
			}
		}
		if(channels.createItem(channel).getStatusCode() >= 400)
			throw new BadRequestException();
	}

	public ChannelDAO getChannelById(String id) {
		init();
		if(cache!=null) {
			String res = cache.getResource().get(CHANNEL + id);
			if (res != null) {
				try {
					return new ObjectMapper().readValue(res, ChannelDAO.class);
				} catch (JsonProcessingException e) {
					e.printStackTrace();
				}
			}
		}
		return channels.queryItems("SELECT * FROM Channels WHERE Channels.id=\"" + id + "\"",
				new CosmosQueryRequestOptions(), ChannelDAO.class).stream().findFirst()
				.orElseThrow(NotFoundException::new);
	}

	public List<ChannelDAO> getDeletedChannels() {
		init();
		return channels.queryItems("SELECT * FROM Channels WHERE Channels.garbage=true OFFSET " + 0 + " LIMIT " + 100,
						new CosmosQueryRequestOptions(), ChannelDAO.class).stream().collect(Collectors.toList());
	}

	public void updateChannel(ChannelDAO channel) {
		init();
		if(cache!=null) {
			try {
				cache.getResource().set(CHANNEL + channel.getId(), new ObjectMapper().writeValueAsString(channel));
			} catch (JsonProcessingException e) {
				e.printStackTrace();
			}
		}
		if(channels.replaceItem(channel, channel.getId(), new PartitionKey(channel.getId()), new CosmosItemRequestOptions()).getStatusCode() >= 400)
			throw new BadRequestException();
	}

	public void close() {
		client.close();
	}

}
