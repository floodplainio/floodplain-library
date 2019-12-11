package com.dexels.neo4j;

import java.io.InputStream;

import org.neo4j.driver.v1.AuthTokens;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;

public class TestNeo {

	@SuppressWarnings("unused")
	public static void main(String[] args) {
		Driver driver = GraphDatabase.driver("bolt://10.0.0.1",AuthTokens.basic("neo4j","neo4j"));
		InputStream is = TestNeo.class.getResourceAsStream("replication.json");
	}

}
