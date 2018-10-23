package io.openmessaging;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

public class StoreMsgRunnable implements Runnable {
	private  FileChannel fileChannel;
	
	private static ByteBuffer writeBuffer = ByteBuffer.allocateDirect(1024 * 1024 * 56);

	public StoreMsgRunnable( FileChannel fileChannel) {
		this.fileChannel = fileChannel;
	}

	@Override
	public synchronized void run() {
//		for (int i = 0; i < 1000000; i++) {
//			String queuename = "Queue-" + i;
//			OneQueue10Msg oneQueue10Msg2 = OneMillionQueue.getMillionQueueHolder().get(queuename);
//			for (int j = 0; j < 10; j++) {
//				try {
//					writeBuffer.put(oneQueue10Msg2.getList().get(0).getMsgContent());
//					oneQueue10Msg2.getList().remove(0);
//				} catch (Exception e) {
//					e.printStackTrace();
//				}
//
//			}
//		}
//		try {
//			writeBuffer.flip();
//			fileChannel.write(writeBuffer);
//		} catch (IOException e) {
//			e.printStackTrace();
//		}
	}

}
