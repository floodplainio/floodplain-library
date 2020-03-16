package com.dexels.kafka.impl;

import io.reactivex.Observable;
import io.reactivex.ObservableOperator;
import io.reactivex.Observer;
import io.reactivex.disposables.Disposable;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicLong;

public class TestOperators {
	@Test
	public void testTake() {
		int size = Observable.range(0, 100)
			.take(10)
			.toList()
			.blockingGet()
			.size();
		Assert.assertEquals(10, size);
	}

	@Test
	public void testCustomTake() {
		int size = Observable.range(0, 100)
			.lift(this.customTake(10))
			.toList()
			.blockingGet()
			.size();
		Assert.assertEquals(10, size);
	}

	
	public ObservableOperator<Integer,Integer> customTake(long count) {
		return new ObservableOperator<Integer,Integer>(){

			@Override
			public Observer<? super Integer> apply(Observer<? super Integer> down) throws Exception {
				AtomicLong processed = new AtomicLong();
				return new Observer<Integer>(){
					Disposable d = null;
					@Override
					public void onComplete() {
						down.onComplete();
					}

					@Override
					public void onError(Throwable t) {
						down.onError(t);
						if(d!=null) {
							d.dispose();
						}
					}

					@Override
					public void onNext(Integer l) {
						long newProcessed = processed.incrementAndGet();
						if(newProcessed > count) {
							d.dispose();
							down.onComplete();
						} else {
							down.onNext(l);
						}
					}

					@Override
					public void onSubscribe(Disposable d) {
						this.d = d;
					}};
			}};
	}
}
