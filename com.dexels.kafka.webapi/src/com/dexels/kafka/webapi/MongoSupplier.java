package com.dexels.kafka.webapi;

public class MongoSupplier {
	
//	private final Map<String,Map<String,Map<String,Object>>> tenantSettings = new HashMap<>();
//	private final Map<String,Map<String,Object>> globalSettings = new HashMap<>();
//	private final Map<String,MongoClient> activeClients = new HashMap<>();
//
//	private final Map<String,Map<String,com.dexels.navajo.mongo.client.MongoClient>> tenantClients = new HashMap<>();
//	private final Map<String,com.dexels.navajo.mongo.client.MongoClient> globalClients = new HashMap<>();
//
//    private Map<String, Generation> generations = new HashMap<>();
//
//	private static MongoSupplier instance;
//	
//	public MongoSupplier() {}
//
//	public void activate() {
//		instance = this;
//	}
//
//	public void deactivate() {
//		instance = null;
//	}
//
//	public static MongoSupplier getInstance() {
//		return instance;
//	}
//	
//    public void addGeneration(Generation generation) {
//        generations.put(generation.getInstance(), generation);
//    }
//    public void removeGeneration(Generation generation) {
//        generations.remove(generation.getInstance());
//    }
//    
//	
//	public void addMongoClient(com.dexels.navajo.mongo.client.MongoClient client, Map<String,Object> baseSettings) {
//		String name = (String) baseSettings.get("name");
//		String tenant = (String) baseSettings.get("instance");
//		String database = (String) baseSettings.get("database");
//		Map<String,Object> settings = new HashMap<>(baseSettings);
//		if(database!=null && database.indexOf("@")!=-1) {
//			database = client.getDatabase();
//			settings.put("database", database);
//		}
//		if (tenant!=null) {
//			Map<String,Map<String,Object>> tSettings = tenantSettings.get(tenant);
//			Map<String,com.dexels.navajo.mongo.client.MongoClient> tClients = tenantClients.get(tenant);
//			if(tSettings == null) {
//				tSettings = new HashMap<>();
//				tenantSettings.put(tenant, tSettings);
//				tClients = new HashMap<>();
//				tenantClients.put(tenant, tClients);
//			}
//			tClients.put(name, client);
//
//			tSettings.put(name,settings);
//		} else {
//			globalSettings.put(name, settings);
//			globalClients.put(name, client);
//		}
//	}
//	
//	public void unbindMongoClient(com.dexels.navajo.mongo.client.MongoClient client, Map<String,Object> baseSettings) {
//	    String name = (String) baseSettings.get("name");
//        String tenant = (String) baseSettings.get("instance");
//        if (tenant!=null) {
//            Map<String,Map<String,Object>> tSettings = tenantSettings.get(tenant);
//            if (tSettings != null) tSettings.remove(name);
//            
//            Map<String,com.dexels.navajo.mongo.client.MongoClient> tClients = tenantClients.get(tenant);
//            if (tClients != null) tClients.remove(name);
//        } else {
//            globalSettings.remove(name);
//            globalClients.remove(name);
//        }
//	}
//	
//	public Optional<MongoDatabase> getDatabase(StreamScriptContext t, String name) {
//		Optional<String> url = getDatabaseURL(t, name);
//		if(!url.isPresent()) {
//			return Optional.empty();
//		}
//		MongoClientURI mci = new MongoClientURI(url.get());
//		return Optional.ofNullable(MongoClients.create(mci.getURI()).getDatabase(mci.getDatabase()));
//	}
//	
//	public Optional<MongoCollection<BsonDocument>> getCollection(StreamScriptContext t, String name, String collection) {
//		Optional<String> url = getDatabaseURL(t, name);
//		if(!url.isPresent()) {
//			return Optional.empty();
//		}
//		MongoClientURI mci = new MongoClientURI(url.get());
//		MongoClient client = MongoClients.create(mci.getURI());
//		Optional<MongoDatabase> database = Optional.ofNullable(client.getDatabase(mci.getDatabase()));
//		if(!database.isPresent()) {
//			client.close();
//			return Optional.empty();
//		}
//		return Optional.ofNullable( database.get().getCollection(collection,BsonDocument.class));
//	}
//	
//	public MongoCollection<BsonDocument> getCollection(String url, String collection) {
//		MongoClientURI mci = new MongoClientURI(url);
//		MongoClient client = MongoClients.create(mci.getURI());
//		Optional<MongoDatabase> database = Optional.ofNullable(client.getDatabase(mci.getDatabase()));
//		return database.get().getCollection(collection,BsonDocument.class); //  Optional.ofNullable( database.get().getCollection(collection,BsonDocument.class));
//	}
//	
//	public Flowable<BsonDocument> queryBlocking(StreamScriptContext t, String name, String collection, Function<com.mongodb.client.MongoCollection<BsonDocument>,Iterable<BsonDocument>> func) {
//		com.dexels.navajo.mongo.client.MongoClient client;
//		String tenant = t.getTenant();
//		if(tenant!=null) {
//			client = tenantClients.getOrDefault(tenant, Collections.emptyMap()).get(name);
//			if(client==null) {
//				client = globalClients.get(name);
//			} else {
//
//			}
//		} else {
//			
//			client = globalClients.get(name);
//		}
//		if(client==null) {
//			return Flowable.error(()->new IllegalStateException("Database missing for resource: "+name));
//		}
//		com.mongodb.client.MongoCollection<BsonDocument> mongoCollection = client.getMongoDatabase().getCollection(collection,BsonDocument.class);
//		return Flowable.fromIterable(func.apply( mongoCollection));
//	}
//	
//	public Flowable<ImmutableMessage> query(StreamScriptContext t, String name, String collection, Function<MongoCollection<BsonDocument>,Publisher<BsonDocument>> func)  {
//		Optional<String> url = getDatabaseURL(t, name);
//		if(!url.isPresent()) {
//			throw new IllegalStateException("MongoDB Resource missing for: "+name);
//		}
//		return query(url.get(),collection,func)
//				.map(StreamMongo::mongoToReplicationMessage);
//	}
//
//	public Flowable<BsonDocument> query(String url, String collection, Function<MongoCollection<BsonDocument>,Publisher<BsonDocument>> func)  {
//		MongoClientURI mci = new MongoClientURI(url);
//		MongoClient client;
//		try {
//			client = createReactiveClient(url, mci,15);
//		} catch (IOException e) {
//			return Flowable.error(()->new IllegalStateException("Cant create client missing mongodb client by url: "+url,e));
//		}
//		Optional<MongoDatabase> database = Optional.ofNullable(client.getDatabase(mci.getDatabase()));
//		if(!database.isPresent()) {
//			client.close();
//			return Flowable.error(()->new IllegalStateException("Database missing for url: "+mci.getURI()));
//		}
//		MongoCollection<BsonDocument> coll = database.get().getCollection(collection,BsonDocument.class);
//		Publisher<BsonDocument> publisher = func.apply(coll);
//		return  Flowable.fromPublisher(publisher);
//	}
//	
//	private MongoClient createReactiveClient(String url, MongoClientURI mci, int maxConcurrent) throws IOException {
//		MongoClient cachedClient = activeClients.get(url);
//		if(cachedClient!=null) {
//			return cachedClient;
//		}
//	    AsynchronousSocketChannelStreamFactoryFactory CUSTOM_FACTORY;
//		CUSTOM_FACTORY = AsynchronousSocketChannelStreamFactoryFactory.builder()
//		        .group(java.nio.channels.AsynchronousChannelGroup.withThreadPool(Executors.newFixedThreadPool(maxConcurrent)))
//		        .build();
//		
//		MongoClientSettings clientSettings = MongoClientSettings.builder()
//				.streamFactoryFactory(CUSTOM_FACTORY)
//				.serverSettings(ServerSettings.builder().applyConnectionString(new ConnectionString(mci.getURI())).build())
//				.clusterSettings(ClusterSettings.builder().applyConnectionString(new ConnectionString(mci.getURI())).build())
//				.build();
//		MongoClient client = cachedClient != null? cachedClient : MongoClients.create(clientSettings);
//			activeClients.put(url, client);
//			System.out.println("Created new client");
////		}
//		return client;
//	}
//
//	public Optional<String> getDatabaseURL(StreamScriptContext t, String name) {
//		String tenant = t.getTenant();
//		if("dummy".equals(name)) {
//			return Optional.of("mongodb://mysql-test1.sportlink-infra.net/KNVB-test-generation-72-total-replication");
//		}
//		if("dummyaaa".equals(name)) {
//			return Optional.of("mongodb://mysql-test1.sportlink-infra.net/aaa_test");
//		}
//		if("dummyexternalstore".equals(name)) {
//			return Optional.of("mongodb://mysql-test1.sportlink-infra.net/test_knvb_externalstore");
//		}
//
//		Map<String,Object> settings; // = tenant == null ? globalSettings.get(name) : tenantSettings.getOrDefault(tenant,Collections.emptyMap()) .get(name);
//		if(tenant==null) {
//			settings = globalSettings.get(name); 
//		} else {
//			settings = tenantSettings.getOrDefault(tenant,Collections.emptyMap()) .get(name);
//		}
//		if(settings==null) {
//			settings = globalSettings.get(name);
//		}
//		if(settings==null) {
//			return Optional.empty();
//		}
//		String url = replaceURLGeneration((String) settings.get("url"),t.getTenant());
//
//		if(url!=null) {
//			return Optional.of(url);
//		}
//		Integer port = (Integer) settings.get("port");
//		String database = (String) settings.get("database");
//		if (database.contains("@")) {
//		    Generation generation = this.generations.get(t.getTenant());
//		    if (generation==null) {
//		        return Optional.empty();
//		    }
//		    database = generation.generationalGroup(database);
//		}
//		url = "mongodb://"+settings.get("host")+(port!=null?":"+port:"")+"/"+database;
//		return Optional.of(url);
//	}
//
//    protected String replaceURLGeneration(String url, String tenant) {
//    	if(url==null) {
//    		return null;
//    	}
//        if (url.contains("@")) {
//
//            int endIndex = url.length();
//            if (url.contains("?")) {
//                endIndex = url.indexOf('?');
//            }
//            String genPart = url.substring(url.indexOf('@'), endIndex);
//            String fullDb = generations.get(tenant).generationalGroup(genPart);
//            url = url.replace(genPart, fullDb);
//            return url;
//        }
//        return url;
//    }

	
}
