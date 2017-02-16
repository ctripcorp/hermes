package com.ctrip.hermes.example;


/**
 * @author Leo Liang(liangjinhua@gmail.com)
 *
 */
public class LocalConsumerBootstrap {

	public static void main(String[] args) {
//		Map<Tpp, MessageRawDataBatch> decodedBatches = new HashMap<>();
//
//		for (Map.Entry<Tpp, MessageRawDataBatch> entry : decodedBatches.entrySet()) {
//			MessageRawDataBatch batch = entry.getValue();
//			List<DecodedMessage> msgs = batch.getMessages();
//			
//			Subscriber s = findSubscriber(entry.getKey(), correlationId);
//			
//			List cmsgs = new ArrayList<>();
//			for (DecodedMessage msg : msgs) {
//				BrokerConsumerMessage<Object> bmsg = new BrokerConsumerMessage<>();
//				bmsg.setBody(decode(s.getMessageClass(), msg.readBody()));
//				bmsg.setKey(msg.getKey());
//				//...
//				cmsgs.add(bmsg);
//			}
//			s.getConsumer().consume(cmsgs);
		}

}
