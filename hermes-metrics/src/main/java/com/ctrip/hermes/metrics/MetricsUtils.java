package com.ctrip.hermes.metrics;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Snapshot;
import com.codahale.metrics.Timer;

public class MetricsUtils {
	public static String printMeter(String name, Meter meter) {
		StringBuilder sb = new StringBuilder();
		sb.append(String.format("              name = %s%n", name));
		sb.append(String.format("             count = %d%n", meter.getCount()));
		sb.append(String.format("         mean rate = %2.2f events/%s%n", meter.getMeanRate(), "s"));
		sb.append(String.format("     1-minute rate = %2.2f events/%s%n", meter.getOneMinuteRate(), "s"));
		sb.append(String.format("     5-minute rate = %2.2f events/%s%n", meter.getFiveMinuteRate(), "s"));
		sb.append(String.format("    15-minute rate = %2.2f events/%s%n", meter.getFifteenMinuteRate(), "s"));
		return sb.toString();
	}

	public static String printHistogram(String name, Histogram histogram) {
		StringBuilder sb = new StringBuilder();
		sb.append(String.format("              name = %s%n", name));
		sb.append(String.format("             count = %d%n", histogram.getCount()));
		Snapshot snapshot = histogram.getSnapshot();
		sb.append(String.format("               min = %d%n", snapshot.getMin()));
		sb.append(String.format("               max = %d%n", snapshot.getMax()));
		sb.append(String.format("              mean = %2.2f%n", snapshot.getMean()));
		sb.append(String.format("            stddev = %2.2f%n", snapshot.getStdDev()));
		sb.append(String.format("            median = %2.2f%n", snapshot.getMedian()));
		sb.append(String.format("              75%% <= %2.2f%n", snapshot.get75thPercentile()));
		sb.append(String.format("              95%% <= %2.2f%n", snapshot.get95thPercentile()));
		sb.append(String.format("              98%% <= %2.2f%n", snapshot.get98thPercentile()));
		sb.append(String.format("              99%% <= %2.2f%n", snapshot.get99thPercentile()));
		sb.append(String.format("            99.9%% <= %2.2f%n", snapshot.get999thPercentile()));
		return sb.toString();
	}

	public static String printTimer(String name, Timer timer) {
		StringBuilder sb = new StringBuilder();
		final Snapshot snapshot = timer.getSnapshot();
		sb.append(String.format("              name = %s%s", name));
		sb.append(String.format("             count = %d%n", timer.getCount()));
		sb.append(String.format("         mean rate = %2.2f calls/%s%n", timer.getMeanRate(), "s"));
		sb.append(String.format("     1-minute rate = %2.2f calls/%s%n", timer.getOneMinuteRate(), "s"));
		sb.append(String.format("     5-minute rate = %2.2f calls/%s%n", timer.getFiveMinuteRate(), "s"));
		sb.append(String.format("    15-minute rate = %2.2f calls/%s%n", timer.getFifteenMinuteRate(), "s"));

		sb.append(String.format("               min = %2.2f %s%n", snapshot.getMin(), "s"));
		sb.append(String.format("               max = %2.2f %s%n", snapshot.getMax(), "s"));
		sb.append(String.format("              mean = %2.2f %s%n", snapshot.getMean(), "s"));
		sb.append(String.format("            stddev = %2.2f %s%n", snapshot.getStdDev(), "s"));
		sb.append(String.format("            median = %2.2f %s%n", snapshot.getMedian(), "s"));
		sb.append(String.format("              75%% <= %2.2f %s%n", snapshot.get75thPercentile(), "s"));
		sb.append(String.format("              95%% <= %2.2f %s%n", snapshot.get95thPercentile(), "s"));
		sb.append(String.format("              98%% <= %2.2f %s%n", snapshot.get98thPercentile(), "s"));
		sb.append(String.format("              99%% <= %2.2f %s%n", snapshot.get99thPercentile(), "s"));
		sb.append(String.format("            99.9%% <= %2.2f %s%n", snapshot.get999thPercentile(), "s"));
		return sb.toString();
	}
}
