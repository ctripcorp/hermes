package com.ctrip.hermes.metaserver.meta;

import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Test;
import org.unidal.lookup.ComponentTestCase;

import com.ctrip.hermes.meta.entity.Endpoint;
import com.ctrip.hermes.meta.entity.Meta;
import com.ctrip.hermes.meta.entity.Server;
import com.ctrip.hermes.meta.transform.DefaultSaxParser;

public class MetaMegerTest extends ComponentTestCase {

	@Test
	public void testMergeNull() throws Exception {
		MetaMerger merger = new MetaMerger();

		Meta base = DefaultSaxParser.parse(this.getClass().getResourceAsStream("base.xml"));

		Meta merged = merger.merge(base, null, null, null);

		assertEquals(base, merged);
	}

	@Test
	public void testMergeNewServers() throws Exception {
		MetaMerger merger = new MetaMerger();

		Meta base = DefaultSaxParser.parse(this.getClass().getResourceAsStream("base.xml"));
		List<Server> newServers = Arrays.asList(makeServer("1.1.1.1", 1111), makeServer("2.2.2.2", 2222));

		Meta merged = merger.merge(base, newServers, null, null);
		Meta expected = DefaultSaxParser.parse(this.getClass().getResourceAsStream("result_of_merge_newServer.xml"));

		assertEquals(expected, merged);
	}

	@Test
	public void testMergeNewEndpoints() throws Exception {
		MetaMerger merger = new MetaMerger();

		Meta base = DefaultSaxParser.parse(this.getClass().getResourceAsStream("base.xml"));

		List<Server> newServers = Arrays.asList(makeServer("1.1.1.1", 1111), makeServer("2.2.2.2", 2222));

		Map<String, Map<Integer, Endpoint>> partition2Endpoint = new HashMap<>();
		Map<Integer, Endpoint> m1 = new HashMap<>();
		m1.put(0, makeEndpoint("br0", "5.5.5.5", 5555));
		partition2Endpoint.put("order_new", m1);

		Map<Integer, Endpoint> m2 = new HashMap<>();
		m2.put(0, makeEndpoint("br1", "6.6.6.6", 6666));
		partition2Endpoint.put("cmessage_fws", m2);

		Meta merged = merger.merge(base, newServers, partition2Endpoint, null);

		Meta expected = DefaultSaxParser.parse(this.getClass().getResourceAsStream("result_of_merge_all.xml"));

		assertEquals(expected, merged);
	}

	@Test
	public void testMergeAll() throws Exception {
		MetaMerger merger = new MetaMerger();

		Meta base = DefaultSaxParser.parse(this.getClass().getResourceAsStream("base.xml"));

		Map<String, Map<Integer, Endpoint>> partition2Endpoint = new HashMap<>();
		Map<Integer, Endpoint> m1 = new HashMap<>();
		m1.put(0, makeEndpoint("br0", "5.5.5.5", 5555));
		partition2Endpoint.put("order_new", m1);

		Map<Integer, Endpoint> m2 = new HashMap<>();
		m2.put(0, makeEndpoint("br1", "6.6.6.6", 6666));
		partition2Endpoint.put("cmessage_fws", m2);

		Meta merged = merger.merge(base, null, partition2Endpoint, null);

		Meta expected = DefaultSaxParser.parse(this.getClass().getResourceAsStream("result_of_merge_newEndpoints.xml"));

		assertEquals(expected, merged);
	}

	private Endpoint makeEndpoint(String id, String host, int port) {
		Endpoint e = new Endpoint();
		e.setHost(host);
		e.setId(id);
		e.setPort(port);
		e.setType(Endpoint.BROKER);

		return e;
	}

	private Server makeServer(String host, int port) {
		Server s = new Server();
		s.setHost(host);
		s.setId(host + ":" + port);
		s.setPort(port);

		return s;
	}

}
