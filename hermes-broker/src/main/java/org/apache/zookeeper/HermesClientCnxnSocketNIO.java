package org.apache.zookeeper;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.zookeeper.ClientCnxn.Packet;

/**
 * @author Leo Liang(liangjinhua@gmail.com)
 *
 */
public class HermesClientCnxnSocketNIO extends ClientCnxnSocketNIO {

	private static final AtomicBoolean m_paused = new AtomicBoolean(false);

	public HermesClientCnxnSocketNIO() throws IOException {
		super();
	}

	public static void pause() {
		m_paused.set(true);
	}

	public static void resume() {
		m_paused.set(false);
	}

	public static boolean isPaused() {
		return m_paused.get();
	}

	@Override
	void doTransport(int waitTimeOut, List<Packet> pendingQueue, LinkedList<Packet> outgoingQueue, ClientCnxn cnxn)
	      throws IOException, InterruptedException {
		if (m_paused.get()) {
			throw new IOException("Zookeeper paused.");
		} else {
			super.doTransport(waitTimeOut, pendingQueue, outgoingQueue, cnxn);
		}
	}

}
