package com.ctrip.hermes.broker.longpolling;

import io.netty.channel.Channel;

import java.util.Date;

import com.ctrip.hermes.core.bo.Offset;
import com.ctrip.hermes.core.bo.Tpg;
import com.ctrip.hermes.core.lease.Lease;

public class PullMessageTask {
	private Tpg m_tpg;

	private long m_correlationId;

	private int m_pullCommandVersion;

	private Offset m_startOffset;

	private int m_batchSize;

	private Channel m_channel;

	private long m_expireTime;

	private Lease m_brokerLease;

	private boolean m_withOffset;

	private String m_clientIp;

	private String m_filter;

	private Date m_receiveTime;

	private Date m_pullTime;

	public PullMessageTask(Date receiveTime, Date pullTime) {
		m_receiveTime = receiveTime;
		m_pullTime = pullTime;
	}

	public Date getPullTime() {
		return m_pullTime;
	}

	public String getClientIp() {
		return m_clientIp;
	}

	public void setClientIp(String clientIp) {
		m_clientIp = clientIp;
	}

	public void setTpg(Tpg tpg) {
		m_tpg = tpg;
	}

	public void setFilter(String filter) {
		m_filter = filter;
	}

	public void setCorrelationId(long correlationId) {
		m_correlationId = correlationId;
	}

	public void setPullCommandVersion(int pullCommandVersion) {
		m_pullCommandVersion = pullCommandVersion;
	}

	public void setStartOffset(Offset startOffset) {
		m_startOffset = startOffset;
	}

	public void setBatchSize(int batchSize) {
		m_batchSize = batchSize;
	}

	public void setChannel(Channel channel) {
		m_channel = channel;
	}

	public void setExpireTime(long expireTime) {
		m_expireTime = expireTime;
	}

	public void setBrokerLease(Lease brokerLease) {
		m_brokerLease = brokerLease;
	}

	public long getExpireTime() {
		return m_expireTime;
	}

	public Tpg getTpg() {
		return m_tpg;
	}

	public String getFilter() {
		return m_filter;
	}

	public long getCorrelationId() {
		return m_correlationId;
	}

	public Offset getStartOffset() {
		return m_startOffset;
	}

	public int getBatchSize() {
		return m_batchSize;
	}

	public Channel getChannel() {
		return m_channel;
	}

	public Lease getBrokerLease() {
		return m_brokerLease;
	}

	public int getPullMessageCommandVersion() {
		return m_pullCommandVersion;
	}

	public boolean isWithOffset() {
		return m_withOffset;
	}

	public void setWithOffset(boolean withOffset) {
		m_withOffset = withOffset;
	}

	public Date getReceiveTime() {
		return m_receiveTime;
	}

	@Override
	public String toString() {
		return "PullMessageTask [m_tpg=" + m_tpg + ", m_correlationId=" + m_correlationId + ", m_pullCommandVersion="
		      + m_pullCommandVersion + ", m_startOffset=" + m_startOffset + ", m_batchSize=" + m_batchSize
		      + ", m_channel=" + m_channel + ", m_expireTime=" + m_expireTime + ", m_brokerLease=" + m_brokerLease
		      + ", m_withOffset=" + m_withOffset + ", m_clientIp=" + m_clientIp + ", m_filter=" + m_filter
		      + ", m_receiveTime=" + m_receiveTime + "]";
	}

}
