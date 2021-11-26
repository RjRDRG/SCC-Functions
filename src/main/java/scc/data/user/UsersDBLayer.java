package scc.data.user;

import com.azure.cosmos.*;
import com.azure.cosmos.models.CosmosItemRequestOptions;
import com.azure.cosmos.models.CosmosItemResponse;
import com.azure.cosmos.models.CosmosQueryRequestOptions;
import com.azure.cosmos.models.PartitionKey;
import com.azure.cosmos.util.CosmosPagedIterable;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import redis.clients.jedis.JedisPool;
import scc.cache.RedisCache;
import scc.mgt.AzureProperties;

public class UsersDBLayer {
	private static final String DB_NAME = "scc2122db";
	
	private static UsersDBLayer instance;

	public static synchronized UsersDBLayer getInstance() {
		if(instance == null) {
			try {
				CosmosClient client = new CosmosClientBuilder()
						.endpoint(AzureProperties.getProperty("COSMOSDB_URL"))
						.key(AzureProperties.getProperty("COSMOSDB_KEY"))
						.gatewayMode()		// replace by .directMode() for better performance
						.consistencyLevel(ConsistencyLevel.SESSION)
						.connectionSharingAcrossClientsEnabled(true)
						.contentResponseOnWriteEnabled(true)
						.buildClient();
				JedisPool cache = RedisCache.getCachePool();
				instance = new UsersDBLayer(client,cache);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		return instance;
	}
	
	private final CosmosClient client;
	private CosmosDatabase db;
	private CosmosContainer users;
	private final JedisPool cache;
	
	public UsersDBLayer(CosmosClient client, JedisPool cache) {
		this.client = client;
		this.cache = cache;
	}
	
	private synchronized void init() {
		if(db != null) return;
		db = client.getDatabase(DB_NAME);
		users = db.getContainer("Users");
	}

	public CosmosItemResponse<Object> delUserById(String id) {
		init();
		PartitionKey key = new PartitionKey( id);
		return users.deleteItem(id, key, new CosmosItemRequestOptions());
	}
	
	public CosmosItemResponse<Object> delUser(UserDAO user) {
		init();
		cache.getResource().del("user: " + user.getIdUser());
		return users.deleteItem(user, new CosmosItemRequestOptions());
	}
	
	public CosmosItemResponse<UserDAO> putUser(UserDAO user) {
		init();
		try {
			cache.getResource().set("user:" + user.getIdUser(), new ObjectMapper().writeValueAsString(user));
		} catch (JsonProcessingException e) {
			e.printStackTrace();
		}
		return users.createItem(user);
	}
	
	public CosmosPagedIterable<UserDAO> getUserById( String id) {
		init();
		return users.queryItems("SELECT * FROM Users WHERE Users.id=\"" + id + "\"", new CosmosQueryRequestOptions(), UserDAO.class);
	}
	
	public CosmosItemResponse<UserDAO> updateUser(UserDAO user) {
		init();

		try {
			cache.getResource().set("user:" + user.getIdUser(), new ObjectMapper().writeValueAsString(user));
		} catch (JsonProcessingException e) {
			e.printStackTrace();
		}

		return users.replaceItem(user, user.get_rid(),new PartitionKey(user.getIdUser()), new CosmosItemRequestOptions());
	}

	
	public CosmosPagedIterable<UserDAO> getUsers(int off, int limit) {
		init();
		return users.queryItems("SELECT * FROM Users OFFSET "+off+" LIMIT "+limit, new CosmosQueryRequestOptions(), UserDAO.class);
	}

	
	public void close() {
		client.close();
	}
	
	
}
