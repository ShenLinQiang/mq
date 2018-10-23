package io.openmessaging;

import java.util.LinkedList;
import java.util.List;

public class OneQueue10Msg {
	public String getQueueName() {
		return queueName;
	}

	public void setQueueName(String queueName) {
		this.queueName = queueName;
	}

	public String queueName;
	public List<MsgObject> list = new LinkedList<>();
//	private List<MsgObject> list = new Vector<>();

	public OneQueue10Msg(String queueName) {
		this.queueName = queueName;
	}

	public List<MsgObject> getList() {
		return list;
	}

	public void putMsg(MsgObject msgObject) {
		list.add(msgObject);
	}
}
