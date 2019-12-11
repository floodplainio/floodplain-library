package com.dexels.slack.webhook;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;

import org.osgi.service.component.annotations.Activate;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.ConfigurationPolicy;
import org.osgi.service.component.annotations.Deactivate;
import org.reactivestreams.Subscriber;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dexels.pubsub.rx2.api.MessagePublisher;
import com.dexels.pubsub.rx2.api.PubSubMessage;
import com.dexels.pubsub.rx2.api.TopicPublisher;
import com.github.seratch.jslack.Slack;
import com.github.seratch.jslack.api.webhook.Payload;
import com.github.seratch.jslack.api.webhook.WebhookResponse;

import io.reactivex.Completable;
import io.reactivex.Flowable;

@Component(name="dexels.slack.publisher", configurationPolicy=ConfigurationPolicy.REQUIRE, immediate=true)
public class SlackWebhookPublisher implements TopicPublisher {
    private final static Logger logger = LoggerFactory.getLogger(SlackWebhookPublisher.class);

    private static final String DEFAULT_USERNAME = "DexBot";
    
    private String url;
    private String username = DEFAULT_USERNAME;

    @Activate
    public void activate(Map<String, Object> settings) {
        this.url = (String) settings.get("url");
//        this.type =
        if (settings.containsKey("username")) {
            this.username = (String) settings.get("username");
        }
    }
    
    @Deactivate
    public void deactivate() {
        
    }
    
    @Override
    public MessagePublisher publisherForTopic(String topic) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void publish(String topic, String key, byte[] value) throws IOException {
        Payload payload = Payload.builder()
          .channel(topic)
          .username(username)
          .iconEmoji(":smile_cat:")
          .text(new String(value, "UTF-8"))
          .build();

        Slack slack = Slack.getInstance();
        WebhookResponse response = slack.send(url, payload);
        logger.debug("response: ", response);
    }

    @Override
    public void publish(String topic, String key, byte[] value, Consumer<Object> onSuccess, Consumer<Throwable> onFail) {
        throw new UnsupportedOperationException();
    }

  

    @Override
    public void create(String topic, Optional<Integer> replicationFactor, Optional<Integer> partitionCount) {
        throw new UnsupportedOperationException();
        
    }

    @Override
    public void create(String topic) {
        throw new UnsupportedOperationException();
        
    }

    @Override
    public void delete(String topic) {
        throw new UnsupportedOperationException();
    }
    

    @Override
    public void flush() {
        throw new UnsupportedOperationException();
        
    }

    @Override
    public Subscriber<PubSubMessage> backpressurePublisher(Optional<String> defaultTopic, int maxInFlight) {
        throw new UnsupportedOperationException();
    }

    public static void main(String[] args) {
        SlackWebhookPublisher i = new SlackWebhookPublisher();
        try {
            i.publish("@Stefan", "", "Look at me!".getBytes());
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

	@Override
	public Completable deleteCompletable(List<String> topics) {
		return Completable.complete();
	}

	@Override
	public Flowable<String> streamTopics() {
		return Flowable.empty();
	}
}
