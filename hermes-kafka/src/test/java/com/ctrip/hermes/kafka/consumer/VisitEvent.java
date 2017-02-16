package com.ctrip.hermes.kafka.consumer;

import java.util.Date;

public class VisitEvent {
	String ip;

	Date tz;

	String url;

	public String getIp() {
		return ip;
	}

	public Date getTz() {
		return tz;
	}

	public String getUrl() {
		return url;
	}

	public void setIp(String ip) {
		this.ip = ip;
	}

	public void setTz(Date tz) {
		this.tz = tz;
	}

	public void setUrl(String url) {
		this.url = url;
	}

	public String toString() {
		return new StringBuilder(ip).append(" ").append(tz).append(" ").append(url).toString();
	}
}
