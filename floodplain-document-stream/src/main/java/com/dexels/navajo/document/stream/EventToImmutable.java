package com.dexels.navajo.document.stream;

import com.dexels.immutable.api.ImmutableMessage;
import com.dexels.navajo.document.stream.events.NavajoStreamEvent;
import io.reactivex.Flowable;
import io.reactivex.FlowableTransformer;
import org.reactivestreams.Publisher;

import java.util.Optional;

public class EventToImmutable implements FlowableTransformer<NavajoStreamEvent,ImmutableMessage> {

	private final Optional<String> path;

	public EventToImmutable(Optional<String> path) {
		this.path = path;
	}

	
	@Override
	public Publisher<ImmutableMessage> apply(Flowable<NavajoStreamEvent> flow) {
			return flow
					.lift(NavajoStreamToMutableMessageStream.toMutable(this.path))
					.concatMap(e->e)
					.map(StreamDocument::messageToReplication);
	}


}
