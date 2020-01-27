package com.dexels.kafka.webapi;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import javax.servlet.AsyncContext;
import javax.servlet.Servlet;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.osgi.service.component.annotations.Activate;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.ConfigurationPolicy;
import org.osgi.service.component.annotations.Deactivate;
import org.osgi.service.component.annotations.Reference;
import org.osgi.service.component.annotations.ReferencePolicy;

import com.dexels.pubsub.rx2.api.PersistentPublisher;
import com.dexels.pubsub.rx2.api.PersistentSubscriber;
import com.dexels.pubsub.rx2.api.TopicPublisher;
import com.dexels.replication.api.ReplicationMessage;
import com.dexels.replication.api.ReplicationMessageParser;
import com.dexels.replication.impl.protobuf.FallbackReplicationMessageParser;

import io.reactivex.Completable;
import io.reactivex.Flowable;
import io.reactivex.disposables.Disposable;
import io.reactivex.functions.Action;
import io.reactivex.functions.Predicate;

@Component(configurationPolicy=ConfigurationPolicy.IGNORE,  name="dexels.kafka.mongo.servlet",property={"servlet-name=dexels.kafka.servlet","alias=/kafkamongo","asyncSupported.Boolean=true"},immediate=true,service = { Servlet.class})
public class KafkaDownloadMongoServlet extends HttpServlet {

	private static final long serialVersionUID = 9025834882284006898L;

	private PersistentSubscriber persistentSubscriber;
	private PersistentPublisher publisher;
	private Set<Disposable> running = new HashSet<>();

	@Activate
	public void activate() {
		
	}
	
	@Deactivate
	public void deactivate() {
		running.forEach(e->e.dispose());
	}
	
	@Reference(policy=ReferencePolicy.DYNAMIC, unbind="clearPersistentSubscriber")
	public void setPersistentSubscriber(PersistentSubscriber persistenSubscriber) {
		this.persistentSubscriber = persistenSubscriber;
	}
	
	public void clearPersistentSubscriber(PersistentSubscriber persistenSubscriber) {
		this.persistentSubscriber = null;
	}

	@Reference(policy=ReferencePolicy.DYNAMIC, unbind="clearTopicPublisher")
	public void setTopicPublisher(PersistentPublisher publisher) {
		this.publisher = publisher;
	}
	
	
	public void clearTopicPublisher(TopicPublisher publisher) {
		this.publisher = null;
	}

	
	@Override
	protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
		String host = req.getParameter("host");
		String port = req.getParameter("port")==null?"27017":req.getParameter("port");
		String database = req.getParameter("database");
		String collection = req.getParameter("collection");
		String topic = req.getParameter("topic");
	
		resp.setCharacterEncoding("UTF-8");
		resp.setContentType("text/plain");
		AsyncContext ac = req.startAsync();
		ac.setTimeout(8000000);
		Disposable d = downloadTopicToMongoDb(host, Integer.parseInt(port), database, collection, this.persistentSubscriber,this.publisher, topic, e->true)
			.flatMapCompletable(e->e)
			.doOnComplete(()->{
				resp.getWriter().write("done!");
				resp.getWriter().close();
			})
			.subscribe();
		running.add(d);
	}

	private static Flowable<Completable> downloadTopicToMongoDb(String host, int port, String database, String collection, PersistentSubscriber kts,PersistentPublisher pub, String topic,
			Predicate<ReplicationMessage> filter) throws IOException {
		System.setProperty(ReplicationMessage.PRETTY_JSON, "true");

		ReplicationMessageParser parser = new FallbackReplicationMessageParser(true);

		Map<Integer, Long> offsetMap = kts.partitionOffsets(topic);
		Map<Integer, Long> offsetMapInc = new HashMap<Integer, Long>();
		offsetMap.entrySet().forEach(e -> {
			offsetMapInc.put(e.getKey(), e.getValue() - 1);
		});
		String toTag =kts.encodeTopicTag(offsetMapInc);
		// TODO multiple partitions
		String fromTag = "0:0";
		AtomicLong messageCount = new AtomicLong();
		AtomicLong writtenDataCount = new AtomicLong();
		AtomicLong writtenMessageCount = new AtomicLong();

		final Disposable d = Flowable.interval(2, TimeUnit.SECONDS)
				.doOnNext(e -> System.err.println("In progress. MessageCount: " + messageCount.get()
						+ " writtenMessageCount: " + writtenMessageCount + " written data: " + writtenDataCount.get()))
				.doOnTerminate(() -> System.err.println("Progress complete")).subscribe();
		final String generatedConsumerGroup = UUID.randomUUID().toString();
		Action onTerminate = ()->{
			d.dispose();
			pub.deleteGroups(Arrays.asList(new String[]{generatedConsumerGroup}));
		};
		// TODO rebuild mongo direct here?
		return Flowable.empty();
//		return Flowable.fromPublisher(kts.subscribeSingleRange(topic, generatedConsumerGroup, fromTag, toTag))
//				.subscribeOn(Schedulers.io())
//				.observeOn(Schedulers.io())
//				.concatMapIterable(e -> e)
//				.doOnNext(m -> messageCount.incrementAndGet()).retry(5)
//				.map(e->parser.parseBytes(e))
//				.filter(filter)
//				.compose(MongoDirectSink.createMongoCollection(database,host,""+port,collection))
//				.doOnTerminate(onTerminate)
//				.doOnCancel(onTerminate);
	}

}
