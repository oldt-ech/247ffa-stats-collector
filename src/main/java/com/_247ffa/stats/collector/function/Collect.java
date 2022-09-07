package com._247ffa.stats.collector.function;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpResponse.BodyHandlers;
import java.util.Arrays;
import java.util.Date;
import java.util.UUID;
import java.util.function.Consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com._247ffa.stats.collector.model.Server;
import com._247ffa.stats.collector.model.ServerRecord;
import com.azure.cosmos.CosmosClient;
import com.azure.cosmos.CosmosClientBuilder;
import com.azure.cosmos.CosmosContainer;
import com.azure.cosmos.CosmosDatabase;
import com.azure.cosmos.models.CosmosContainerProperties;
import com.azure.cosmos.models.ThroughputProperties;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;

public class Collect implements Consumer<String> {

	private Logger logger = LoggerFactory.getLogger(Collect.class);
	
	private final String url;
	private final CosmosClient client;
	private final CosmosDatabase database;
	private final CosmosContainer container;
	private final String databaseName;
	private final String databaseContainerName;

	public Collect() {
		url = System.getenv("QE_STATS_ENDPOINT");
		databaseName = "247ffa";
		databaseContainerName = System.getenv("QE_STATS_CONTAINER");
		client = new CosmosClientBuilder()
				.endpoint(System.getenv("DB_HOST"))
				.key(System.getenv("DB_KEY"))
				.buildClient();
		database = client.getDatabase(client.createDatabaseIfNotExists(databaseName).getProperties().getId());
		CosmosContainerProperties containerProperties = new CosmosContainerProperties(databaseContainerName, "/server");
		container = database.getContainer(database
				.createContainerIfNotExists(containerProperties, ThroughputProperties.createManualThroughput(400))
				.getProperties().getId());
	}

	@Override
	public void accept(String timerInfo) {
		try {
			HttpRequest req = HttpRequest.newBuilder().uri(new URI(url)).GET().build();
			HttpClient client = HttpClient.newHttpClient();
			HttpResponse<String> response = client.send(req, BodyHandlers.ofString());
			ObjectMapper om = JsonMapper.builder().findAndAddModules().build();
			Server[] servers = om.readValue(response.body(), Server[].class);
			Date date = new Date();
			UUID uuid = UUID.randomUUID();
			Arrays.asList(servers).stream().map((s) -> {
				ServerRecord sr = new ServerRecord();
				sr.setCurrentPlayers(s.getSession().getValue().getCurrentPlayers());
				sr.setDate(date);
				sr.setName(s.getName());
				sr.setMap(s.getSession().getValue().getMapname());
				sr.setMiniProfileId(s.getId());
				sr.setRequestUUID(uuid);
				sr.setMaxPlayers(s.getSession().getMaxPlayers());
				return sr;
			}).forEach((record) -> container.createItem(record));
		} catch (URISyntaxException | IOException | InterruptedException e) {
			logger.error("Job exception", e);
		} finally {
			client.close();
		}
	}

}