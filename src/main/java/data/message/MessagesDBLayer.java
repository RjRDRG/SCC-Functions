package data.message;

import cache.Cache;
import com.azure.cosmos.*;
import com.azure.cosmos.models.CosmosItemRequestOptions;
import com.azure.cosmos.models.CosmosQueryRequestOptions;
import com.azure.cosmos.models.PartitionKey;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import javax.ws.rs.BadRequestException;
import javax.ws.rs.NotFoundException;
import javax.ws.rs.WebApplicationException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class MessagesDBLayer {
	private static final String DB_NAME = "scc2122db";
	private static final String RECENT_MSGS = "MostRecentMsgs";
	private static final int MAX_MSG_IN_CACHE = 20;

	private final CosmosContainer messages;
	private final JedisPool cache;

	public MessagesDBLayer() {
		CosmosClient client = new CosmosClientBuilder()
				.endpoint(System.getenv("COSMOSDB_URL"))
				.key(System.getenv("COSMOSDB_KEY"))
				.gatewayMode()		// replace by .directMode() for better performance
				.consistencyLevel(ConsistencyLevel.SESSION)
				.connectionSharingAcrossClientsEnabled(true)
				.contentResponseOnWriteEnabled(true)
				.buildClient();
		this.cache = Cache.getInstance();
		CosmosDatabase db = client.getDatabase(DB_NAME);
		this.messages = db.getContainer("Messages");
	}

	public void delMsgById(String id) {
		MessageDAO msg = getMsgById(id);
		if(msg == null)
			throw new NotFoundException();

		PartitionKey key = new PartitionKey(msg.getChannel());
		if(messages.deleteItem(msg.getId(), key, new CosmosItemRequestOptions()).getStatusCode() >= 400)
			throw new BadRequestException();

		if(cache!=null) {
			try(Jedis jedis = cache.getResource()) {
				List<String> msgs = jedis.lrange(RECENT_MSGS + msg.getChannel(), 0, -1);

				if (!msgs.isEmpty()) {
					jedis.del(RECENT_MSGS + msg.getChannel());

					ObjectMapper mapper = new ObjectMapper();

					for (String s : msgs) {
						MessageDAO m = null;
						try {
							m = mapper.readValue(s, MessageDAO.class);
						} catch (JsonProcessingException e) {
							e.printStackTrace();
						}
						if (!m.getId().equals(id)) {
							jedis.lpush(RECENT_MSGS + msg.getChannel(), s);
						}
					}
				}
			}
		}
	}

	public List<MessageDAO> getMsgsSentByUser(String id) {
		return messages.queryItems("SELECT * FROM Messages WHERE Messages.user=\"" + id + "\"", new CosmosQueryRequestOptions(), MessageDAO.class).stream().collect(Collectors.toList());
	}

	public void putMsg(MessageDAO msg) {
		int status = messages.createItem(msg).getStatusCode();
		if(status >= 400) throw new WebApplicationException(status);
	}

	public void updateMessage(MessageDAO msg) {
		if(cache != null) {
			try (Jedis jedis = cache.getResource()) {
				List<String> lst = jedis.lrange(RECENT_MSGS + msg.getChannel(), 0, -1);
				ObjectMapper mapper = new ObjectMapper();
				for (String s : lst) {
					MessageDAO m = null;
					try {
						m = mapper.readValue(s, MessageDAO.class);
					} catch (JsonProcessingException e) {
						e.printStackTrace();
						throw new RuntimeException(e);
					}
					if (msg.equals(m)) {
						delMsgById(m.getId());
						try {
							jedis.lpush(RECENT_MSGS + msg.getChannel(), mapper.writeValueAsString(msg));
						} catch (JsonProcessingException e) {
							e.printStackTrace();
						}
						break;
					}
				}
			}
		}

		int status = messages.replaceItem(msg, msg.getId(), new PartitionKey(msg.getChannel()), new CosmosItemRequestOptions()).getStatusCode();
		if(status >= 400) throw new WebApplicationException(status);
	}
	
	public MessageDAO getMsgById(String id) {
		return messages.queryItems("SELECT * FROM Messages WHERE Messages.id=\"" + id + "\"", new CosmosQueryRequestOptions(), MessageDAO.class).stream().findFirst()
				.orElseThrow(NotFoundException::new);
	}

	public List<MessageDAO> getMessages(String channel, int off, int limit) {
		List<MessageDAO> messageDAOS = new ArrayList<>(limit);
		ObjectMapper m = new ObjectMapper();

		int cachedMessages = 0;

		if(cache!=null) {
			try(Jedis jedis = cache.getResource()) {
				if (off < MAX_MSG_IN_CACHE) {
					List<MessageDAO> cacheMsgs = jedis.lrange(RECENT_MSGS + channel, off, Math.min(limit - 1, MAX_MSG_IN_CACHE - 1)).stream().map(
							s -> {
								try {
									return m.readValue(s, MessageDAO.class);
								} catch (JsonProcessingException e) {
									e.printStackTrace();
									throw new RuntimeException(e);
								}
							}
					).collect(Collectors.toList());
					cachedMessages = cacheMsgs.size();
					messageDAOS.addAll(cacheMsgs);
				}
			}
		}

		if(limit > cachedMessages) {
			messageDAOS.addAll(
					messages.queryItems(
							"SELECT * FROM Messages WHERE Messages.channel=\"" + channel + "\" ORDER BY Messages._ts DESC OFFSET " + (off + cachedMessages) + " LIMIT " + (limit - cachedMessages),
							new CosmosQueryRequestOptions(), MessageDAO.class
					)
					.stream().collect(Collectors.toList())
			);
		}

		return messageDAOS;
	}

	public void deleteChannelsMessages(String channel) {
		List<MessageDAO> messageDAOS = messages.queryItems(
				"SELECT * FROM Messages WHERE Messages.channel=\"" + channel + "\"",
				new CosmosQueryRequestOptions(), MessageDAO.class
		).stream().collect(Collectors.toList());

		for(MessageDAO messageDAO : messageDAOS) {
			messages.deleteItem(messageDAO, new CosmosItemRequestOptions());
		}

		if(cache!=null) {
			cache.getResource().del(RECENT_MSGS + channel);
		}
	}

}
