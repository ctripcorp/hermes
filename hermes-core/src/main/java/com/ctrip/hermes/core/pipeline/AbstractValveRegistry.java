package com.ctrip.hermes.core.pipeline;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;

import org.unidal.lookup.ContainerHolder;
import org.unidal.tuple.Triple;

import com.ctrip.hermes.core.pipeline.spi.Valve;


public class AbstractValveRegistry extends ContainerHolder implements ValveRegistry {

	private SortedSet<Triple<Valve, String, Integer>> m_tuples = new TreeSet<Triple<Valve, String, Integer>>(
	      new Comparator<Triple<Valve, String, Integer>>() {

		      @Override
		      public int compare(Triple<Valve, String, Integer> t1, Triple<Valve, String, Integer> t2) {
			      return t1.getLast().compareTo(t2.getLast());
		      }
	      });

	private List<Valve> m_valves = Collections.emptyList();

	@Override
	public List<Valve> getValveList() {
		return m_valves;
	}

	@Override
	public synchronized void register(Valve valve, String name, int order) {
		m_tuples.add(new Triple<Valve, String, Integer>(valve, name, order));

		ArrayList<Valve> newValves = new ArrayList<Valve>();
		for (Triple<Valve, String, Integer> triple : m_tuples) {
			newValves.add(triple.getFirst());
		}

		m_valves = newValves;
	}

	protected void doRegister(String id, int order) {
		register(lookup(Valve.class, id), id, order);
	}
}
