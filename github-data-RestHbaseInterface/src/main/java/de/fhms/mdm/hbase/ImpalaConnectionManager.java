package de.fhms.mdm.hbase;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

public class ImpalaConnectionManager {

	private static final String CONNECTION_URL_PROPERTY = "connection.url";
	private static final String JDBC_DRIVER_NAME_PROPERTY = "jdbc.driver.class.name";

	private static String connectionUrl;
	private static String jdbcDriverName;

	public ImpalaConnectionManager() {
		try {
			loadConfiguration();
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	private void loadConfiguration() throws IOException {
		InputStream input = null;
		try {
			String filename = ClouderaImpalaJdbcExample.class.getSimpleName() + ".conf";
			input = ClouderaImpalaJdbcExample.class.getClassLoader().getResourceAsStream(filename);
			Properties prop = new Properties();
			prop.load(input);

			connectionUrl = prop.getProperty(CONNECTION_URL_PROPERTY);
			jdbcDriverName = prop.getProperty(JDBC_DRIVER_NAME_PROPERTY);
		} finally {
			try {
				if (input != null) {
					input.close();
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	public String executeImpalaQuery(String query) throws IOException {

		System.out.println("\n=============================================");
		System.out.println("Using Connection URL: " + connectionUrl);
		System.out.println("Running Query: " + query);

		Connection con = null;
		ResultSet resultSet = null;
		String resultString = "";
		try {
			Class.forName(jdbcDriverName);
			con = DriverManager.getConnection(connectionUrl);
			Statement stmt = con.createStatement();
			resultSet = stmt.executeQuery(query);
			System.out.println("\n== Return Query Result.... ");
			while (resultSet.next()) {
				resultString += resultSet.getString(1);
				resultString += "_";
				resultString += resultSet.getString(2);
				resultString += "_";
				resultString += resultSet.getString(3);
			}
		} catch (SQLException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				con.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		return resultString;
	}
}
