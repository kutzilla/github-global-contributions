package de.fhms.mdm.hbase.tests;

import static org.junit.Assert.*;

import java.util.List;

import org.junit.Test;

import de.fhms.mdm.hbase.HBaseConnectionManager;
import de.fhms.mdm.hbase.data.AllCommitData;
import de.fhms.mdm.hbase.data.City;
import de.fhms.mdm.hbase.data.Country;

public class HBaseConnectionTests {
	
	@Test
	public void testGetRepositories() {
		HBaseConnectionManager cManager = new HBaseConnectionManager();
		List<String> result = cManager.getRepositories();
		assertTrue(result.size()>0);
	}
	
	
	@Test
	public void testGetCommits() {
		HBaseConnectionManager cManager = new HBaseConnectionManager();

		AllCommitData result = cManager.getAllCommits(null, 0, 0);
//		List<Commit> result = cManager.getAllCommits("kutzilla/marlin-cm", 1451676520, System.currentTimeMillis());
		City[] cities = result.getCities();
		Country[] countries = result.getCountries();
		System.out.println("Cities");
		for (int i = 0; i < cities.length; i++) {
			System.out.println(cities[i]);
		}
		System.out.println("countries");
		for (int i = 0; i < countries.length; i++) {
			System.out.println(countries[i]);
		}
		assertTrue(result != null);
	}

}
