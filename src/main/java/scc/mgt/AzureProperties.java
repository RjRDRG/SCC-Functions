package scc.mgt;

import java.io.FileInputStream;
import java.util.Properties;

public class AzureProperties
{
	public static final String PROPS_FILE = "azurekeys-westeurope.props";
	private static Properties props;

	private static synchronized Properties getProperties() {
		if( props == null || props.size() == 0) {
			props = new Properties();
			try {
				props.load( new FileInputStream("WEB-INF/" + PROPS_FILE));
			} catch (Exception e) {
				// do nothing
			}
		}
		return props;
	}

	public static String getProperty(String key) {
		return getProperties().getProperty(key);
	}
}
