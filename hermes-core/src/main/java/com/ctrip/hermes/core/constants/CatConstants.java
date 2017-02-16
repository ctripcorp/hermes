package com.ctrip.hermes.core.constants;

public interface CatConstants {

	public static final String ROOT_MESSAGE_ID = "RootMessageId";

	public static final String CURRENT_MESSAGE_ID = "CurrentMessageId";

	public static final String SERVER_MESSAGE_ID = "ServerMessageId";

	public static final String TYPE_REMOTE_CALL = "RemoteCall";

	public static final String TYPE_MESSAGE_DELIVER_DB = "Message.Deliver.DB.";

	public static final String TYPE_MESSAGE_DELIVER = "Message.Deliver";

	public static final String TYPE_MESSAGE_DELIVER_BY_SAFE = "Message.Deliver.SafeTrigger-";

	public static final String TYPE_MESSAGE_DELIVER_BY_PRIORITY = "Message.Deliver.PriorityTrigger-";

	public static final String TYPE_MESSAGE_DELIVER_BY_NONPRIORITY = "Message.Deliver.NonPriorityTrigger-";

	public static final String TYPE_MESSAGE_MISS_RATIO = "Hermes.KPI.Miss.Ratio";

	public static final String TYPE_MESSAGE_CONSUME_LATENCY = "Message.Consume.Latency";

	public static final String TYPE_MESSAGE_CONSUME_RESEND_LATENCY = "Message.Consume.Resend.Latency";

	public static final String TYPE_MESSAGE_CONSUMED = "Message.Consumed";

	public static final String TYPE_MESSAGE_PRODUCE_QUEUE_EXPIRED = "Message.Produce.Queue.Expired";

	public static final String TYPE_MESSAGE_PRODUCE_ELAPSE = "Message.Produce.Elapse";

	public static final String TYPE_MESSAGE_PRODUCE_BY_PRIORITY = "Message.Produce.PriorityTrigger-";

	public static final String TYPE_MESSAGE_PRODUCE_BY_NONPRIORITY = "Message.Produce.NonPriorityTrigger-";

	public static final String TYPE_MESSAGE_CONSUME_POLL_ELAPSE = "Message.Consume.Poll.Elapse";

	public static final String TYPE_MESSAGE_CONSUME_POLL_TRIED = "Message.Consume.Poll.Tried";

	public static final String TYPE_MESSAGE_CONSUME_COLLECT_ELAPSE = "Message.Consume.Collect.Elapse";

	public static final String TYPE_MESSAGE_CONSUME_COLLECT_TRIED = "Message.Consume.Collect.Tried";

	public static final String TYPE_MESSAGE_PRODUCE_ELAPSE_LARGE = "Message.Produce.Elapse.Large";

	public static final String TYPE_MESSAGE_PRODUCE_TRANSPORT = "Message.Produce.Transport";

	public static final String TYPE_MESSAGE_PRODUCE_TIMEOUT = "Message.Produce.Timeout";

	public static final String TYPE_MESSAGE_PRODUCE_TRANSPORT_SKIP = "Message.Produce.Transport.Skip";

	public static final String TYPE_MESSAGE_PRODUCE_ACKED = "Message.Produce.Acked";

	public static final String TYPE_MESSAGE_PRODUCE_TRIED = "Message.Produce.Tried";

	public static final String TYPE_MESSAGE_BROKER_FLUSH = "Message.Flush";

	public static final String TYPE_MESSAGE_BROKER_PRODUCE_DB = "Message.Produce.DB.";

	public static final String TYPE_MESSAGE_BROKER_PRODUCE = "Message.Produce";

	public static final String TYPE_MESSAGE_BROKER_PRODUCE_BYTES = "Message.Produce.Bytes.";

	public static final String TYPE_MESSAGE_CONSUME_ACK_TRANSPORT = "Message.Consume.Ack.Transport";

	public static final String TYPE_SEND_CMD = "Send.Cmd.V";

	public static final String TYPE_PULL_CMD = "Pull.Cmd.V";

	public static final String TYPE_HERMES_CMD_VERSION = "Hermes.Command.Version";

	public static final String TYPE_CMD_DROP = "Hermes.Command.Drop";

	public static final String TYPE_HERMES_BILL = "Hermes.Bill";

	public static final String NAME_HERMES_BILL_MYSQL = "Hermes.Bill.MySQL";

	public static final String NAME_HERMES_BILL_KAFKA = "Hermes.Bill.Kafka";

	public static final String TYPE_HERMES_BILL_UNKNOWN = "Hermes.Bill.Unknown";

	public static final String TYPE_HERMES_CLIENT_VERSION = "Hermes.Client.Version";

	public static final String TYPE_MESSAGE_BROKER_QPS_RATE_LIMIT_EXCEED = "Message.Rate.Limit.QPS";

	public static final String TYPE_MESSAGE_BROKER_BYTES_RATE_LIMIT_EXCEED = "Message.Rate.Limit.Bytes";

	public static final String TRANSACTION_FAIL = "1";

	public static final String TYPE_LEASE_DIRTY_PERSIST = "Lease.Dirty.Persist";

	public static final String TYPE_LEASE_DIRTY_LOAD = "Lease.Dirty.Load";

	public static final String TYPE_LEASE_ACQUIRE_CONSUMER = "Lease.Acquire.Consumer";

	public static final String TYPE_LEASE_ACQUIRED_CONSUMER = "Lease.Acquired.Consumer";

	public static final String TYPE_LEASE_RENEW_CONSUMER = "Lease.Renew.Consumer";

	public static final String TYPE_LEASE_RENEWED_CONSUMER = "Lease.Renewed.Consumer";

	public static final String TYPE_LEASE_ACQUIRE_BROKER = "Lease.Acquire.Broker";

	public static final String TYPE_LEASE_ACQUIRED_BROKER = "Lease.Acquired.Broker";

	public static final String TYPE_LEASE_RENEW_BROKER = "Lease.Renew.Broker";

	public static final String TYPE_LEASE_RENEWED_BROKER = "Lease.Renewed.Broker";

	public static final String TYPE_LEASE_PROXY = "Lease.ProxyPass";

	public static final String TYPE_ROLE_CHANGED = "Role.Changed";

	public static final String TYPE_ACK_FLUSH = "Message.Ack.Flush.DB.";

	public static final String TYPE_SERVER_INBOUND_ERROR = "Server.Inbound.Error";
}
