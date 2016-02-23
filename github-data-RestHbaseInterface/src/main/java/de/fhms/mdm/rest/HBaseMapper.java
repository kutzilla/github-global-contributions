package de.fhms.mdm.rest;

import java.util.List;

import de.fhms.mdm.hbase.HBaseConnectionManager;
import de.fhms.mdm.hbase.data.Repo;

public class HBaseMapper {

	public static Repo[] getRepositories() {
		HBaseConnectionManager connMan = new HBaseConnectionManager();
		List<String> repos = connMan.getRepositories();
		Repo[] resultArr = new Repo[repos.size() ];
//		resultArr[0] = new Repo("Alle");
		for (int i = 0; i < repos.size(); i++) {
			resultArr[i] = new Repo(repos.get(i));
		}
		return resultArr;
	}
}
