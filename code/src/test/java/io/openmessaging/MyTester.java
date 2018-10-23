package io.openmessaging;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

public class MyTester {

	public static void main(String[] args) throws Exception {

		long start = System.currentTimeMillis();
		for (int i = 0; i < 1000000; i++) {
			String queueName = "Queue-"+i;
			OneQueue10Msg oneQueue10Msg = new OneQueue10Msg(queueName);
			for(int j=0;j<10;j++) {
				MsgObject msgObject = new MsgObject();
				msgObject.setMsgContent(gen(3).getBytes());
				oneQueue10Msg.putMsg(msgObject);
				
			}
			 OneMillionQueue.putOneQueue(oneQueue10Msg);

		}
		long end = System.currentTimeMillis();
		System.out.println("time " + (end - start)/1000 + " s");

	}

	public static String gen(int length) {
		char[] ss = new char[length];
		int i = 0;
		while (i < length) {
			int f = (int) (Math.random() * 3);
			if (f == 0)
				ss[i] = (char) ('A' + Math.random() * 26);
			else if (f == 1)
				ss[i] = (char) ('a' + Math.random() * 26);
			else
				ss[i] = (char) ('0' + Math.random() * 10);
			i++;
		}
		String is = new String(ss);
		return is;
	}
}
