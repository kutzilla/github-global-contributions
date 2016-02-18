package de.fhms.mdm.hbase;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;

public class ImpalaQueryHandler {

	public static String getExampleQuery() {
		ImpalaConnectionManager connectionManager = new ImpalaConnectionManager();
		String query = "select * from tab1;";
		String resultString = "result:";
		try {
			resultString = connectionManager.executeImpalaQuery(query);
//			if (resultSet != null) {
//				while (resultSet.next()) {
//					resultString += resultSet.getString(1);
//					resultString += "_";
//					resultString += resultSet.getString(2);
//					resultString += "_";
//					resultString += resultSet.getString(3);
//				}
//			}
		} catch (IOException e) {
			e.printStackTrace();
		} 
		return resultString;
	}

}
