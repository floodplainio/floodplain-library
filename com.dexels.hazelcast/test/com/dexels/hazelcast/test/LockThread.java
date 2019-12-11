package com.dexels.hazelcast.test;

import java.util.concurrent.locks.Lock;

class LockThread extends Thread {

	public boolean result;

	private Lock myLock;

	private boolean block;
	
	public LockThread(Lock l) {
		myLock = l;
		this.block = false;
	}
	
	public LockThread(Lock l, boolean block) {
		myLock = l;
		this.block = block;
	}

	@Override
	public void run() {
		if ( block ) {
			long start = System.currentTimeMillis();
			System.err.println("TRYING TO ACQUIRE LOCK...");
			myLock.lock();
			System.err.println("... GOT IT AFTER" + ( System.currentTimeMillis() - start ) + " millis.");
			result = true;
			myLock.unlock();
		} else {
			result = myLock.tryLock();
			if ( result ) {
				myLock.unlock();
			}
		}
	}
}
