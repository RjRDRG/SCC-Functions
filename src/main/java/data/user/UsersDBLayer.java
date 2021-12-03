package data.user;

import cache.Cache;
import com.azure.cosmos.*;
import com.azure.cosmos.models.CosmosItemRequestOptions;
import com.azure.cosmos.models.CosmosQueryRequestOptions;
import com.azure.cosmos.models.PartitionKey;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import data.authentication.Session;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import javax.ws.rs.BadRequestException;
import javax.ws.rs.NotAuthorizedException;
import javax.ws.rs.NotFoundException;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Cookie;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

public class UsersDBLayer {
	private static final String DB_NAME = "scc2122db";
	public static final String USER = "user:";

	private final CosmosContainer users;
	private final JedisPool cache;
	
	public UsersDBLayer() {
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
		users = db.getContainer("Users");
	}

	public void discardUserById(String id) {
		UserDAO userDAO = getUserById(id);
		userDAO.setGarbage(true);
		updateUser(userDAO);
	}

	public void delUserById(String id) {
		if(cache!=null) {
			try(Jedis jedis = cache.getResource()) {
				jedis.del(USER + id);
			}
		}
		PartitionKey key = new PartitionKey(id);
		if(users.deleteItem(id, key, new CosmosItemRequestOptions()).getStatusCode() >= 400)
			throw new BadRequestException();
	}
	
	public void createUser(UserDAO user) {
		if(cache!=null) {
			try(Jedis jedis = cache.getResource()){
				jedis.set(USER + user.getId(), new ObjectMapper().writeValueAsString(user));
			} catch (JsonProcessingException e) {
				e.printStackTrace();
			}
		}

		int status = users.createItem(user).getStatusCode();
		if(status >= 400) throw new WebApplicationException(status);
	}
	
	public UserDAO getUserById(String id) {
		if(cache!=null) {
			try(Jedis jedis = cache.getResource()) {
				String res = jedis.get(USER + id);
				if (res != null) {
					try {
						return new ObjectMapper().readValue(res, UserDAO.class);
					} catch (JsonProcessingException e) {
						e.printStackTrace();
					}
				}
			}
		}
		return users.queryItems("SELECT * FROM Users WHERE Users.id=\"" + id + "\"", new CosmosQueryRequestOptions(), UserDAO.class)
				.stream().findFirst().orElseThrow(NotFoundException::new);
	}
	
	public void updateUser(UserDAO user) {
		if(cache!=null) {
			try(Jedis jedis = cache.getResource()) {
				jedis.set(USER + user.getId(), new ObjectMapper().writeValueAsString(user));
			} catch (JsonProcessingException e) {
				e.printStackTrace();
			}
		}

		int status = users.replaceItem(user, user.getId(),new PartitionKey(user.getId()), new CosmosItemRequestOptions()).getStatusCode();
		if(status >= 400) throw new WebApplicationException(status);
	}

	
	public List<UserDAO> getUsers(int off, int limit) {
		return users.queryItems("SELECT * FROM Users OFFSET "+off+" LIMIT "+limit, new CosmosQueryRequestOptions(), UserDAO.class).stream().collect(Collectors.toList());
	}

    public List<UserDAO> getDeletedUsers() {
		return users.queryItems("SELECT * FROM Users WHERE Users.garbage=true", new CosmosQueryRequestOptions(), UserDAO.class).stream().collect(Collectors.toList());
	}

	public void putSession(Session s) {
		if(cache!=null) {
			try (Jedis jedis = cache.getResource()) {
				jedis.set(s.getSessionId(), new ObjectMapper().writeValueAsString(s));
			} catch (JsonProcessingException e) {
				e.printStackTrace();
				throw new RuntimeException(e);
			}
		}
	}

	public Session getSession(String sessionID) {
		try(Jedis jedis = cache.getResource()) {
			return new ObjectMapper().readValue(jedis.get(sessionID), Session.class);
		} catch (Exception e) {
			e.printStackTrace();
			throw new NotAuthorizedException("No valid session initialized");
		}
	}

	/**
	 * Throws exception if not appropriate user for operation on Channel
	 */
	public void checkCookieUser(Cookie session, String[] ids) throws NotAuthorizedException {
		boolean enable = Boolean.parseBoolean(
				Optional.ofNullable(System.getenv("ENABLE_AUTH")).orElse("false")
		);
		enable = enable && Boolean.parseBoolean(
				Optional.ofNullable(System.getenv("ENABLE_CACHE")).orElse("true")
		);
		if(!enable) return;

		Session s = checkCookieUser(session);
		if (s.getIdUser().equals("admin")) return;

		for (String id : ids) {
			if (s.getIdUser().equals(id)) return;
		}
		throw new NotAuthorizedException("Invalid user : " + s.getIdUser());
	}

	/**
	 * Throws exception if not appropriate user for operation on Channel
	 */
	public void checkCookieUser(Cookie session, String id) throws NotAuthorizedException {
		boolean enable = Boolean.parseBoolean(
				Optional.ofNullable(System.getenv("ENABLE_AUTH")).orElse("false")
		);
		enable = enable && Boolean.parseBoolean(
				Optional.ofNullable(System.getenv("ENABLE_CACHE")).orElse("true")
		);
		if(!enable) return;

		Session s = checkCookieUser(session);
		if (!s.getIdUser().equals(id) && !s.getIdUser().equals("admin"))
			throw new NotAuthorizedException("Invalid user : " + s.getIdUser());
	}

	/**
	 * Throws exception if not appropriate user for operation on Channel
	 */
	private Session checkCookieUser(Cookie session) throws NotAuthorizedException {
		if (session == null || session.getValue() == null)
			throw new NotAuthorizedException("No session initialized");
		Session s = getSession(session.getValue());
		if (s == null || s.getIdUser() == null || s.getIdUser().length() == 0)
			throw new NotAuthorizedException("No valid session initialized");
		return s;
	}
}
