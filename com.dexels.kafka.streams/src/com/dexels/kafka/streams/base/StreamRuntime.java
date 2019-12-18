package com.dexels.kafka.streams.base;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.util.Arrays;
import java.util.Collections;
import java.util.Dictionary;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

import org.apache.kafka.streams.Topology;
import org.osgi.framework.InvalidSyntaxException;
import org.osgi.service.cm.Configuration;
import org.osgi.service.cm.ConfigurationAdmin;
import org.osgi.service.component.annotations.Activate;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.Deactivate;
import org.osgi.service.component.annotations.Reference;
import org.osgi.service.component.annotations.ReferencePolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dexels.http.reactive.elasticsearch.ElasticSink;
import com.dexels.http.reactive.graph.NeoSink;
import com.dexels.kafka.streams.api.StreamTopologyException;
import com.dexels.kafka.streams.api.sink.Sink;
import com.dexels.kafka.streams.api.sink.SinkConfiguration;
import com.dexels.kafka.streams.base.StreamInstance.SinkType;
import com.dexels.kafka.streams.processor.generic.GenericProcessorBuilder;
import com.dexels.kafka.streams.sink.mongo.MongoDirectSink;
import com.dexels.kafka.streams.transformer.custom.CommunicationTransformer;
import com.dexels.kafka.streams.transformer.custom.CopyFieldTransformer;
import com.dexels.kafka.streams.transformer.custom.CreateCoordinateTransformer;
import com.dexels.kafka.streams.transformer.custom.CreateListTransformer;
import com.dexels.kafka.streams.transformer.custom.CreatePublicIdTransformer;
import com.dexels.kafka.streams.transformer.custom.FormatGenderTransformer;
import com.dexels.kafka.streams.transformer.custom.FormatZipCodeTransformer;
import com.dexels.kafka.streams.transformer.custom.MergeDateTimeTransformer;
import com.dexels.kafka.streams.transformer.custom.SplitToListTransformer;
import com.dexels.kafka.streams.transformer.custom.StringToDateTransformer;
import com.dexels.kafka.streams.transformer.custom.TeamName;
import com.dexels.kafka.streams.xml.parser.CaseSensitiveXMLElement;
import com.dexels.kafka.streams.xml.parser.XMLElement;
import com.dexels.kafka.streams.xml.parser.XMLParseException;
import com.dexels.navajo.repository.api.RepositoryInstance;
import com.dexels.replication.transformer.api.MessageTransformer;

import io.reactivex.Observable;
import io.reactivex.disposables.Disposable;
import io.reactivex.schedulers.Schedulers;
import io.reactivex.subjects.PublishSubject;
import io.reactivex.subjects.Subject;

@Component(name="kafka.stream.runtime", service = {StreamRuntime.class}, immediate=true)
public class StreamRuntime {

	private static final Logger logger = LoggerFactory.getLogger(StreamRuntime.class);
	private final Map<String,StreamInstance> streams = new HashMap<>();
	
	private RepositoryInstance repositoryInstance;
	private StreamConfiguration configuration;
	private final List<String> instanceFilter;
	private final Set<StreamInstance> startedInstances = new HashSet<>();
	Subject<Runnable> updateQueue = PublishSubject.<Runnable>create().toSerialized();
	private Disposable updateQueueSubscription;
	private final Map<SinkType,Sink> sinkRegistry = new EnumMap<>(SinkType.class);

	private final Map<String,MessageTransformer> transformerRegistry = new HashMap<>();
	private final Map<String,GenericProcessorBuilder> genericProcessorRegistry = new HashMap<>();
	private ConfigurationAdmin configAdmin;
	
	public StreamRuntime() {
		String filter = System.getenv("FILTER_INSTANCES");
		if(filter==null || "".equals(filter)) {
			this.instanceFilter = Collections.emptyList();
		} else {
			this.instanceFilter = Arrays.asList(filter.split(","));
		}
		transformerRegistry.put("formatgender", new FormatGenderTransformer());
		transformerRegistry.put("createlist", new CreateListTransformer());
		transformerRegistry.put("splitToList", new SplitToListTransformer());
		transformerRegistry.put("copyfield", new CopyFieldTransformer());
		transformerRegistry.put("publicid", new CreatePublicIdTransformer());
		transformerRegistry.put("formatcommunication", new CommunicationTransformer());
		transformerRegistry.put("stringtodate", new StringToDateTransformer());
        transformerRegistry.put("formatzipcode", new FormatZipCodeTransformer());
		transformerRegistry.put("teamname", new TeamName());
		transformerRegistry.put("mergedatetime", new MergeDateTimeTransformer());
        transformerRegistry.put("createcoordinate", new CreateCoordinateTransformer());
		transformerRegistry.put("fieldtoupper", (params,msg)->msg.with(params.get("field"), ((String)(msg.columnValue(params.get("field")))).toString().toUpperCase(),"string"));
		transformerRegistry.put("fieldtolower", (params,msg)->msg.with(params.get("field"), ((String)(msg.columnValue(params.get("field")))).toString().toLowerCase(),"string"));
		transformerRegistry.put("emailtolower", (params,msg)->{
				if("EMAIL".equals(msg.columnValue("typeofcommunication"))) {
					return msg.with("communicationdata", ((String)msg.columnValue("communicationdata")).toLowerCase() , "string");
				} else {
					return msg;
				}});
		this.updateQueueSubscription = updateQueue.observeOn(Schedulers.from(Executors.newSingleThreadExecutor()))
			.subscribe(r->r.run());
		
		sinkRegistry.put(SinkType.ELASTICSEARCHDIRECT, new ElasticSink());
		sinkRegistry.put(SinkType.MONGODBDIRECT, new MongoDirectSink());
		sinkRegistry.put(SinkType.NEO4J, new NeoSink());
	}

	@Activate
	public void activate() throws IOException, InterruptedException, ExecutionException {
		File resources = new File(this.repositoryInstance.getRepositoryFolder(),"config/resources.xml");
		try (Reader r = new FileReader(resources)){
			this.configuration = parseConfig(this.repositoryInstance.getDeployment(),r,configAdmin);
		} catch (XMLParseException | IOException e) {
			logger.error("Error starting streaminstance", e);
			return;
		}

		
		parseFromRepository();
		logger.info("Streams: {}",streams.keySet());
		startInstances();
	}
	
	private synchronized void startInstances() {
		streams.entrySet().forEach(e->{
			StreamInstance in = e.getValue();
			if(startedInstances.contains(in)) {
				logger.warn("Instance: {} has started already, ignoring restart",e.getKey());
			} else {
				if(in==null) {
					logger.warn("Whoa! StreamInstance not found: {}, ignoring",e.getKey());
				} else {
					startedInstances.add(in);
					logger.info("Starting instance: {} from thread: {}",e.getKey(),Thread.currentThread().getName());
					new Thread(()->in.start()).start();
				}
			}
			
		});
		
	}

	@Reference(target="(name=teamorder)")
	public void setTeamOrder(MessageTransformer teamOrder) {
		this.transformerRegistry.put("teamorder", teamOrder);
	}

	@Reference(target="(name=normalizeteamname)")
	public void setNormalizer(MessageTransformer normalizeTeam) {
		this.transformerRegistry.put("normalizeteamname", normalizeTeam);
	}

	@Reference(target="(name=dexels.debezium.processor)")
	public void setDebeziumProcessor(GenericProcessorBuilder processor) {
		this.genericProcessorRegistry.put("debezium", processor);
	}

	@Reference(unbind="clearConfigAdmin",policy=ReferencePolicy.DYNAMIC)
	public void setConfigAdmin(ConfigurationAdmin configAdmin) {
		this.configAdmin = configAdmin;
	}

	/**
	 * @param configAdmin the configAdmin to remove 
	 */
	public void clearConfigAdmin(ConfigurationAdmin configAdmin) {
		this.configAdmin = null;
	}


	
	public static StreamConfiguration parseConfig(String deployment,Reader r, ConfigurationAdmin configAdmin) {
		try {
			XMLElement xe = new CaseSensitiveXMLElement();
			xe.parseFromReader(r);
			final Optional<StreamConfiguration> found = xe.getChildren()
				.stream()
				.filter(elt->"deployment".equals( elt.getName()))
				.filter(elt->deployment.equals(elt.getStringAttribute("name")))
				.map(elt->toStreamConfig(deployment,elt,configAdmin))
				.findFirst();
			if(!found.isPresent()) {
				throw new StreamTopologyException("No configuration found for deployment: "+deployment);
			}
			return found.get();
			
		} catch (XMLParseException | IOException e) {
			throw new StreamTopologyException("Configuration error for deployment: "+deployment,e);
		}
		
	}

	private static StreamConfiguration toStreamConfig(String deployment,XMLElement deploymentXML, ConfigurationAdmin configAdmin) {
		String kafka = deploymentXML.getStringAttribute("kafka");
		int maxWait = deploymentXML.getIntAttribute("maxWait", 5000);
		int maxSize = deploymentXML.getIntAttribute("maxSize", 100);
		int replicationFactor = deploymentXML.getIntAttribute("replicationFactor", 1);
		Map<String,SinkConfiguration> sinks = deploymentXML.getChildren()
			.stream()
			.map(elt->new SinkConfiguration(elt.getName(),elt.getStringAttribute("name"), elt.attributes()))
			.collect(Collectors.toMap(e->e.name(), e->e));
		final StreamConfiguration streamConfiguration = new StreamConfiguration(kafka, sinks,deployment,maxWait,maxSize,replicationFactor);
		if(configAdmin!=null) {
			registerSinks(streamConfiguration,configAdmin);
		}
		return streamConfiguration;
	}

	private static  void registerSinks(StreamConfiguration conf, ConfigurationAdmin configAdmin) {
		conf.sinks().entrySet().forEach(e->{
			String sinkResource = "dexels.streams.sink";
			Dictionary<String,Object> settings = new Hashtable<String,Object>(e.getValue().settings());
			final String name = e.getValue().name();
			final String type = e.getValue().type();
			settings.put("name", name);
			settings.put("type", type);
			try {
				Configuration cf = createOrReuse(sinkResource,"(name="+name+")",configAdmin);
				updateIfChanged(cf,settings);
			} catch (Exception e1) {
				logger.error("Error: ", e1);
			}
			
		});
	}
	
	protected static Configuration createOrReuse(String pid, final String filter, ConfigurationAdmin configAdmin)
			throws IOException {
		Configuration cc = null;
		try {
			Configuration[] c = configAdmin.listConfigurations(filter);
			if(c!=null && c.length>1) {
				logger.warn("Multiple configurations found for filter: {}", filter);
			}
			if(c!=null && c.length>0) {
				cc = c[0];
			}
		} catch (InvalidSyntaxException e) {
			logger.error("Error in filter: {}",filter,e);
		}
		if(cc==null) {
			cc = configAdmin.createFactoryConfiguration(pid,null);
		}
		return cc;
	}
	
    private static void updateIfChanged(Configuration c, Dictionary<String,Object> settings) throws IOException {
		Dictionary<String,Object> old = c.getProperties();
		if(old!=null) {
			if(!old.equals(settings)) {
				c.update(settings);
			}
		} else {
			c.update(settings);
		}
	}

	
	private void parseFromRepository() throws IOException, InterruptedException, ExecutionException {
		File repo = this.repositoryInstance.getRepositoryFolder();
		File output = getOutputFolder();
		if(!output.exists()) {
			output.mkdirs();
		}
		File streamFolder = new File(repo,"streams");
		File[] folders =  streamFolder.listFiles(file->file.isDirectory());
		String generationEnv = System.getenv("GENERATION");
		
		if(generationEnv==null || "".equals(generationEnv)) {
			throw new IllegalArgumentException("Can not load stream instance: no generation");
		}
		Topology topology = new Topology();
		for (File folder : folders) {
			if(folder.getName().startsWith(".") || folder.getName().startsWith("_")) {
				// ignore "_"
				continue;
			}
			File[] f =  folder.listFiles((file,name)->name.endsWith(".xml"));
			for (File file : f) {
			    try {
			        parseFile(topology,output, streamFolder, file, generationEnv);
			    } catch (Throwable t) {
			        logger.error("Error in parsing. Ignoring path: "+ file.getAbsolutePath(),t);
					if(System.getenv("DRAMA_MODE")!=null) {
						System.exit(-1);
					}
			    }
				
			}
			
		}
		File[] f =  streamFolder.listFiles((file,name)->name.endsWith(".xml"));
		for (File file : f) {
			parseFile(topology,output, streamFolder, file,generationEnv);
		}
	}

	private void parseFile(Topology unusedTopology, File outputFolder, File streamFolder, File file, String generation) throws IOException, InterruptedException, ExecutionException {
		String pathInStreamsFolder = streamFolder.toPath().relativize(file.toPath()).toString();
		logger.info("Parsing replication file at path: {}",pathInStreamsFolder);
	
		String name = nameFromFileName(pathInStreamsFolder);

		if(!this.instanceFilter.isEmpty() && !this.instanceFilter.contains(name)) {
			logger.info(" -> Skipping non-matching instance: {}", name);
			return;
		}
		addStreamInstance(unusedTopology,file, streamFolder,outputFolder,generation);
	}

	public static String nameFromFileName(String fullpath) {
		String path = fullpath.split("\\.")[0];
		String[] pathelements = path.split("/");
		String[] pathparts = pathelements[pathelements.length-1].split("-");
		return pathparts.length > 1 ? pathparts[0] :pathelements[pathelements.length-1];
	}

	private void addStreamInstance(Topology unusedGlobaltopology, File file, File streamFolder, File outputStorage, String generation) throws IOException, InterruptedException, ExecutionException {
		String pathInStreamsFolder = streamFolder.toPath().relativize(file.toPath()).toString();
		logger.info("Parsing replication file at path: {}",pathInStreamsFolder);
		String name = nameFromFileName(pathInStreamsFolder);
		if(file.length()==0) {
			logger.warn("Ignoring empty file: {}",file.getAbsolutePath());
			return;
		}
		ClassLoader original = Thread.currentThread().getContextClassLoader();
		try(InputStream definitionStream = new FileInputStream(file)) {
			Thread.currentThread().setContextClassLoader(getClass().getClassLoader());
			StreamInstance si = new StreamInstance(friendlyName(name), Optional.of(this.configuration),this.transformerRegistry,this.genericProcessorRegistry,this.sinkRegistry);
			Topology topology = new Topology();
			si.parseStreamMap(topology,definitionStream,outputStorage,this.repositoryInstance.getDeployment(),generation,Optional.of(file));
			streams.put(friendlyName(name),si);
		} finally {
			Thread.currentThread().setContextClassLoader(original);
		}
	}
	
	private String friendlyName(String name) {
		if(name.endsWith(".xml")) {
			name = name.substring(0, name.length()-4);
		}
		return name.replaceAll("/", "-");
	}

	@Reference(policy=ReferencePolicy.DYNAMIC,unbind="clearRepositoryInstance")
	public void setRepositoryInstance(RepositoryInstance instance) {
		this.repositoryInstance = instance;
	}

	public void clearRepositoryInstance(RepositoryInstance instance) {
		this.repositoryInstance = null;
	}

    @Deactivate
    public void deactivate() {
    	logger.info("Starting deactivate of Kafka Streams");
    	if(this.updateQueueSubscription!=null) {
    		this.updateQueueSubscription.dispose();
    	}
		final ClassLoader original = Thread.currentThread().getContextClassLoader();
		try {
			Thread.currentThread().setContextClassLoader(this.getClass().getClassLoader());
			
			Observable.fromIterable(streams.entrySet())
			.observeOn(Schedulers.newThread())
			.subscribe(si -> {
			    try {
                    si.getValue().shutdown();
                } catch (Throwable e) {
                    logger.error("Error shutting down instance: "+si.getValue(),e );
                }
			    
			});
			
			

			streams.clear();
			this.startedInstances.clear();
		} finally {
			Thread.currentThread().setContextClassLoader(original);
		}
		try {
			Thread.sleep(5000);
		} catch (InterruptedException e) {
			logger.error("Error: ", e);
		}
    	logger.info("Deactivate of Kafka Streams complete");
    }


	private File getOutputFolder() {
		return new File(this.repositoryInstance.getOutputFolder(),"storage");
	}

    public Map<String,StreamInstance> getStreams() {
        return streams;
        
    }
}
