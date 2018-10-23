package io.openmessaging;

import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 这是一个简单的基于内存的实现，以方便选手理解题意； 实际提交时，请维持包名和类名不变，把方法实现修改为自己的内容；
 */
public class DefaultQueueStoreImpl extends QueueStore {
	private static ConcurrentHashMap<Integer, FileChannel> fileHolder = new ConcurrentHashMap<>();
	private static AtomicInteger dataFileCounter = new AtomicInteger(0);
	private static AtomicInteger writeCounter = new AtomicInteger();
	private static ByteBuffer writeBuffer = ByteBuffer.allocateDirect(1024);

	private static String getDataFileName() {
//		return "/Users/jerryq/Desktop/MQ/alidata1/race2018/data/" + dataFileCounter.getAndIncrement() + ".log";
		return "/alidata1/race2018/data/" + dataFileCounter.getAndIncrement() + ".log";

	}

	static {
		try {
			RandomAccessFile randomAccessFile = new RandomAccessFile(getDataFileName(), "rw");
			FileChannel channel = randomAccessFile.getChannel();
			fileHolder.put(dataFileCounter.get() - 1, channel);
			
			for(int i = 0;i<1000000;i++) {
				String queueName = "Queue-"+i;
				OneMillionQueue.getMillionQueueHolder().put(queueName, new OneQueue10Msg(queueName));
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static boolean flag = true;
	public static long start = 0;
	
	@Override
	 void put(String queueName, byte[] message) {
		if(flag==true) {
			start = System.currentTimeMillis();
			flag=false;
		}
		
		MsgObject msgObject = new MsgObject();
		msgObject.setQueueName(queueName);
		msgObject.setMsgContent(message);
		
		OneQueue10Msg oneQueue10Msg = OneMillionQueue.getMillionQueueHolder().get(queueName);
		synchronized (oneQueue10Msg) {
			oneQueue10Msg.putMsg(msgObject);
			if (oneQueue10Msg.getList().size() == 10) {
				
				Integer fullQueue = OneMillionQueue.putOneQueue(oneQueue10Msg);
				if (fullQueue == 1000000) {
					
					long end = System.currentTimeMillis();
					System.out.println("put message  "+(end - start)/1000+" s");
					flag=true;
					
					FileChannel fileChannel = fileHolder.get(dataFileCounter.get() - 1);
					
					for (int i = 0; i < 1000000; i++) {
						String queuename = "Queue-" + i;
						OneQueue10Msg oneQueue10Msg2 = OneMillionQueue.getMillionQueueHolder().get(queuename);
						for (int j = 0; j < 10; j++) {
								writeBuffer.put(oneQueue10Msg2.getList().get(0).getMsgContent());
								oneQueue10Msg2.getList().remove(0);
						}
						try {
							writeBuffer.flip();
							fileChannel.write(writeBuffer);
							writeBuffer.clear();
						}catch (Exception e) {
							e.printStackTrace();
						}
					}
					
					long end2 = System.currentTimeMillis();
					System.out.println("force message  "+(end2 - end)/1000+" s");
					
					if (writeCounter.incrementAndGet() == 2) {
						try {
							writeCounter.set(0);
							RandomAccessFile randomAccessFile = new RandomAccessFile(getDataFileName(), "rw");
							FileChannel channel = randomAccessFile.getChannel();
							fileHolder.put(dataFileCounter.get() - 1, channel);
						} catch (Exception e) {
							e.printStackTrace();
						}
					}
				}
			}
		}
		
	}

	@Override
	synchronized Collection<byte[]> get(String queueName, long offset, long num) {
		return null;
		// queueName = "Queue-233941";
		// offset = 40;

		String[] split = queueName.split("-");
		int queueNum = Integer.valueOf(split[1]);
		int offset1 = (int) offset;
		int fileNum = offset1 / 20;
		int offsetIn20Num = offset1 - 20 * (offset1 / 20);
		byte[] msgs = new byte[10 * 58];
		ByteBuffer buffer = ByteBuffer.allocate(580);
		ArrayList<byte[]> arrayList = new ArrayList<>();
		FileChannel fileChannel = fileHolder.get(fileNum);
		if (offsetIn20Num == 0) {
			int OneFromOffset = queueNum * 58 * 10 + (offsetIn20Num % 10) * 58;
			int len1 = 580;
			fileChannel.read(buffer, OneFromOffset);
		}
		if (offsetIn20Num == 10) {
			int OneFromOffset = queueNum * 58 * 10 + (offsetIn20Num % 10) * 58 + 58 * 10000000;
			int len1 = 580;
			fileChannel.read(buffer, OneFromOffset);
		}
		if (offsetIn20Num < 10) {
			int OneFromOffset = queueNum * 58 * 10 + (offsetIn20Num % 10) * 58;
			int len1 = (10 - offsetIn20Num % 10) * 58;
			
			mappedByteBuffer.position(OneFromOffset);
			mappedByteBuffer.get(msgs, 0, len1);

			int twoFromOffset = 58 * 10000000 + queueNum * 58 * 10;
			int len2 = (offsetIn20Num % 10) * 58;
			mappedByteBuffer.position(twoFromOffset);
			mappedByteBuffer.get(msgs, len1, len2);
		}
		if (offsetIn20Num > 10) {
			int OneFromOffset = queueNum * 58 * 10 + (offsetIn20Num % 10) * 58 + 58 * 10000000;
			int len1 = (10 - offsetIn20Num % 10) * 58;
			mappedByteBuffer.position(OneFromOffset);
			mappedByteBuffer.get(msgs, 0, len1);

			MappedByteBuffer mappedByteBuffer2 = fileHolder.get(fileNum + 1);
			int twoFromOffset = queueNum * 58 * 10;
			int len2 = (offsetIn20Num % 10) * 58;
			mappedByteBuffer2.position(twoFromOffset);
			mappedByteBuffer2.get(msgs, len1, len2);
		}
		for (int i = 0; i < 10; i++) {
			byte[] b = new byte[58];
			System.arraycopy(msgs, i * 58, b, 0, 58);
			arrayList.add(b);
			// System.out.println("queueName:"+queueName+",offset:" + (offset1 + i) +
			// ",msg:" + new String(b));
		}

		return arrayList;
	}

}
