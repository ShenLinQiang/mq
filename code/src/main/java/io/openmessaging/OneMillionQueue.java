package io.openmessaging;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class OneMillionQueue {
	public static ConcurrentHashMap<String, OneQueue10Msg> millionQueueHolder = new ConcurrentHashMap<>();
	public static AtomicInteger queueCounter = new AtomicInteger();

	public static Integer putOneQueue(OneQueue10Msg oneQueue10Msg) {
		millionQueueHolder.put(oneQueue10Msg.getQueueName(), oneQueue10Msg);
		int incrementAndGet = queueCounter.incrementAndGet();
		if (incrementAndGet == 1000001) {
			queueCounter.set(1);
		}
		return queueCounter.get();
	}

	public static ConcurrentHashMap<String, OneQueue10Msg> getMillionQueueHolder() {
		return millionQueueHolder;
	}

	public static void setMillionQueueHolder(ConcurrentHashMap<String, OneQueue10Msg> millionQueueHolder) {
		OneMillionQueue.millionQueueHolder = millionQueueHolder;
	}

}
