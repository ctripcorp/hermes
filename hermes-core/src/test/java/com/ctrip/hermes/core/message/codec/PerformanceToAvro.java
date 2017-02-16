package com.ctrip.hermes.core.message.codec;

import org.unidal.lookup.ComponentTestCase;

public class PerformanceToAvro extends ComponentTestCase {
//	private final int bodyLength = 1000;
//
//	private final int messageCount = 10 * 1000;
//
//	@Test
//	public void runCompareTest() throws IOException, InterruptedException {
//		List<Message> avroMessages = new ArrayList<>();
//		List<ProducerMessage<byte[]>> hermesMessages = new ArrayList<>();
//
//		for (int i = 0; i < messageCount; i++) {
//			String topic = "test.topic.a.topic.name." + i;
//			String key = "topic.key.with.this.message." + i;
//			String partition = "some.partition.on.kafka." + i;
//			byte[] bytes = buildByteBuffer(bodyLength);
//
//			ByteBuffer bf = ByteBuffer.allocate(bodyLength);
//			bf.put(bytes);
//			Map<String, String> properties = buildMap();
//
//			avroMessages.add(new Message(topic, key, partition, new Date().getTime(), true, bf, properties));
//
//			ProducerMessage<byte[]> hermesMessage = new ProducerMessage<>();
//			hermesMessage.setTopic(topic);
//			hermesMessage.setKey(key);
//			hermesMessage.setPartition(partition);
//			hermesMessage.setBody(bytes);
//			hermesMessage.setAppProperties(convertMap(properties));
//			hermesMessages.add(hermesMessage);
//		}
//
//		System.out.println("Initiation is done. Serialize [" + messageCount + "] Messages with message body length of "
//		      + bodyLength + ", " + "priorities map<String, String> size of 100.");
//		runAvro(avroMessages);
//
//		runHermes(hermesMessages);
//		Thread.sleep(50);
//	}
//
//	private void runHermes(List<ProducerMessage<byte[]>> msgs) throws IOException {
//		DefaultProducerMessageCodec msgCodec = (DefaultProducerMessageCodec) lookup(ProducerMessageCodec.class);
//
//		ByteBuf buf = Unpooled.buffer();
//		HermesPrimitiveCodec codec = new HermesPrimitiveCodec(buf);
//
//		long startTime = new Date().getTime();
//		for (ProducerMessage<byte[]> msg : msgs) {
//			msgCodec.write(msg, msg.getBody(), codec);
//		}
//		long seEndTime = new Date().getTime();
//		long fileSize = buf.capacity(); // in bytes
//		codec.bufFlip();
//		List<ProducerMessage<byte[]>> outputMsgs = new ArrayList<>();
//		for (int i = 0; i < messageCount; i++) {
//			outputMsgs.add(msgCodec.read(codec));
//		}
//		long deEndTime = new Date().getTime();
//
//		assertHermosListEquals(msgs, outputMsgs);
//		outputResult("[Hermes]\t", seEndTime - startTime, deEndTime - seEndTime, fileSize);
//	}
//
//	private void assertHermosListEquals(List<ProducerMessage<byte[]>> list1,
//	      List<ProducerMessage<byte[]>> list2) {
//		assertEquals(list1.size(), list2.size());
//
//		for (int i = 0; i < list1.size(); i++) {
//			ProducerMessage<byte[]> m1 = list1.get(i);
//			ProducerMessage<byte[]> m2 = list2.get(i);
//			assertEquals(m1.getTopic(), m2.getTopic());
//			assertEquals(m1.getKey(), m2.getKey());
//			assertEquals(m1.getPartition(), m2.getPartition());
//			assertEquals(m1.getBornTime(), m2.getBornTime());
//			assertTrue(Arrays.equals(m1.getBody(), m2.getBody()));
//			Map<String, Object> map1 = m1.getAppProperties();
//			Map<String, Object> map2 = m2.getAppProperties();
//			assertEquals(map1.size(), map2.size());
//			assertEquals(map1, map2);
//		}
//	}
//
//	private void runAvro(List<Message> msgs) throws IOException {
//		DatumWriter<Message> userDatumWriter = new SpecificDatumWriter<>(Message.class);
//		userDatumWriter.setSchema(msgs.get(0).getSchema());
//
//		ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
//		OutputStream out = new DataOutputStream(byteStream);
//		BinaryEncoder encoder = EncoderFactory.get().blockingBinaryEncoder(out, null);
//
//		long startTime = new Date().getTime();
//		for (Message msg : msgs) {
//			userDatumWriter.write(msg, encoder);
//		}
//
//		long seEndTime = new Date().getTime();
//		long fileSize = byteStream.size();
//
//		List<Message> outputAvroMsgs = new ArrayList<>();
//		DatumReader<Message> userDatumReader = new SpecificDatumReader<>(Message.class);
//		userDatumReader.setSchema(msgs.get(0).getSchema());
//
//		InputStream input = new ByteArrayInputStream(byteStream.toByteArray());
//		Message msg = null;
//		BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(input, null);
//
//		while (!decoder.isEnd()) {
//			outputAvroMsgs.add(userDatumReader.read(msg, decoder));
//		}
//
//		long deEndTime = new Date().getTime();
//		assertListEquals(msgs, outputAvroMsgs);
//
//		outputResult("[Avro]\t", seEndTime - startTime, deEndTime - seEndTime, fileSize);
//	}
//
//	private void assertListEquals(List<Message> list1, List<Message> list2) {
//		assertEquals(list1.size(), list2.size());
//
//		for (int i = 0; i < list1.size(); i++) {
//			Message m1 = list1.get(i);
//			Message m2 = list2.get(i);
//			assertEquals(m1.getTopic(), m2.getTopic().toString());
//			assertEquals(m1.getKey(), m2.getKey().toString());
//			assertEquals(m1.getPartition(), m2.getPartition().toString());
//			assertEquals(m1.getBornTime(), m2.getBornTime());
//			assertEquals(m1.getBody(), m2.getBody());
//			Map<java.lang.String, java.lang.String> map1 = m1.getProperties();
//			Map<java.lang.String, java.lang.String> map2 = m2.getProperties();
//			assertEquals(map1.size(), map2.size());
//
//			// for (CharSequence key : map1.keySet()) {
//			// assertEquals(map1.get(key), map2.get(key.toString()));
//			// }
//		}
//	}
//
//	private void outputResult(String type, long se, long de, long fileSize) {
//		System.out.println(String.format(type + " Serializing: %d(ms), Deserializing: %d(ms), Se-FileSize: %d(byte), "
//		      + "Size Per Message: %d(byte).", se, de, fileSize, fileSize / messageCount));
//	}
//
//	private Map<String, Object> convertMap(Map<String, String> properties) {
//		Map<String, Object> map = new HashMap<>();
//		for (Map.Entry<String, String> entry : properties.entrySet()) {
//			map.put(entry.getKey(), entry.getValue());
//		}
//		return map;
//	}
//
//	private Map<String, String> buildMap() {
//		Map<String, String> map = new HashMap<>();
//		for (int i = 0; i < 100; i++) {
//			map.put(String.valueOf(i), String.valueOf(i * i * i));
//		}
//		return map;
//	}
//
//	private byte[] buildByteBuffer(int bodyLength) {
//		byte[] bytes = new byte[bodyLength];
//		for (int i = 0; i < bodyLength; i++) {
//			bytes[i] = (byte) new Random().nextInt();
//		}
//		return bytes;
//	}
}
