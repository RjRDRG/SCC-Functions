package scc.mgt;

import java.io.FileInputStream;
import java.util.Map;
import java.util.Properties;

import static java.util.Map.entry;

public class AzureProperties
{
	public static final String PROPS_FILE = "azurekeys-westeurope.props";
	private static Properties props;
	private static final Map<String, String> PROPS = Map.ofEntries(
			entry("BlobStoreConnection", "DefaultEndpointsProtocol=https;AccountName=sccstorewesteuroperjr;AccountKey=/9YegBOThddNbvPoT7VhbIbuGIw1yburg3nZSxjDnzp4vIi74xiSNSqkVP9kMBKAIutMaFWo62lVKzT04H6Q4w==;EndpointSuffix=core.windows.net"),
			entry("COSMOSDB_KEY", "Tj4Z2ns0LuWfjBBUe1ZOwsYk8QNWmDjB5DVDXM1GU5bbUcpDaXFCpreW71P2O4ZBAx5E3ZjEEVoKcdYS0Gkrhw=="),
			entry("COSMOSDB_URL", "https://scc2122rjr.documents.azure.com:443/"),
			entry("COSMOSDB_DATABASE", "scc2122db"),
			entry("REDIS_KEY", "WSQ623WwJAjxgi8G83w9ihbV1hGBwq8zAAzCaLltc6E="),
			entry("REDIS_URL", "rediswesteuroperjr.redis.cache.windows.net")
	);

	private static synchronized Properties getProperties() {
		if( props == null || props.size() == 0) {
			props = new Properties();
			try {
				props.load( new FileInputStream("PROPS/" + PROPS_FILE));
			} catch (Exception e) {
				// do nothing
			}
		}
		return props;
	}

	public static String getProperty(String key) {
		String val = null;
		try {
			val = System.getenv( key);
		} catch( Exception e) {
			// do nothing
		}
		if( val != null)
			return val;
		val = getProperties().getProperty(key);
		if(val != null)
			return val;
		return PROPS.get(key);
	}
}
