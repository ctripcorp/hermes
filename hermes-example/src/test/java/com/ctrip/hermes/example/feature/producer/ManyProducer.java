package com.ctrip.hermes.example.feature.producer;

import org.unidal.lookup.ComponentTestCase;

public class ManyProducer extends ComponentTestCase {
//    private final int msgBatchCount = 100;
//    private final int producerCount = 6;
//    MQFactory factory;
//
//    @Before
//    public void init() throws ComponentLookupException {
//        factory = getContainer().lookup(MQFactory.class);
//    }
//
//    /**
//     * 同时发送:
//     */
//    @Test
//    public void sendAtSameTime() throws ComponentLookupException, InterruptedException {
//        final List<Message> receiveMsgs = new ArrayList<>();
//        final List<Message> sendMsgs = new ArrayList<>();
//        final CountDownLatch latch = new CountDownLatch(msgBatchCount * producerCount);
//
//        final List<OldProducer> oldProducerList = new ArrayList<>();
//
//        OldConsumer oldConsumer = buildConsumer(factory);
//        oldConsumer.setMessageListener(new MessageListener() {
//            @Override
//            public void onMessage(Message msg) {
//                receiveMsgs.add(msg);
//                latch.countDown();
//            }
//        });
//        oldConsumer.start();
//
//        for (int i = 0; i < producerCount; i++) {
//            if (i % 2 == 0) {
//                oldProducerList.add(buildSyncProducer(factory));
//            } else {
//                oldProducerList.add(buildAsyncProducer(factory));
//            }
//        }
//
//        for (int i = 0; i < producerCount; i++) {
//            new Thread(new Runnable() {
//                @Override
//                public void run() {
//                    sendMsgs.addAll(batchSendMsgs(
//                            oldProducerList.get(new Random().nextInt(producerCount)), msgBatchCount));
//                }
//            }).start();
//        }
//
//        Assert.assertTrue(latch.await(10, TimeUnit.SECONDS));
//        assertMsgSetEqual(new HashSet<>(sendMsgs), new HashSet<>(receiveMsgs));
//
//        oldConsumer.stop();
//    }
//
//
//    /**
//     * 同时发送不同优先级
//     */
//    @Test
//    public void sendWithPriority() throws ComponentLookupException, InterruptedException {
//        OldProducer syncLow = buildSyncProducer(factory);
//        OldProducer syncMiddle = buildSyncProducer(factory);
//        OldProducer syncHigh = buildSyncProducer(factory);
//        OldProducer asyncLow = buildAsyncProducer(factory);
//        OldProducer asyncMiddle = buildAsyncProducer(factory);
//        OldProducer asyncHigh = buildAsyncProducer(factory);
//
//        final List<Message> receivedMsgs = new ArrayList<>();
//        final List<Message> sendMsgs = new ArrayList<>();
//        final CountDownLatch latch = new CountDownLatch(msgBatchCount * 6);
//        OldConsumer oldConsumer = buildConsumer(factory);
//        oldConsumer.setMessageListener(new MessageListener() {
//            @Override
//            public void onMessage(Message msg) {
//                receivedMsgs.add(msg);
//                latch.countDown();
//            }
//        });
//        oldConsumer.start();
//        oldConsumer.stop();
//
//        sendMsgs.addAll(batchSendMsgs(syncLow, msgBatchCount, MessagePriority.LOW));
//        sendMsgs.addAll(batchSendMsgs(syncMiddle, msgBatchCount, MessagePriority.MIDDLE));
//        sendMsgs.addAll(batchSendMsgs(syncHigh, msgBatchCount, MessagePriority.HIGH));
//        sendMsgs.addAll(batchSendMsgs(asyncLow, msgBatchCount, MessagePriority.LOW));
//        sendMsgs.addAll(batchSendMsgs(asyncMiddle, msgBatchCount, MessagePriority.MIDDLE));
//        sendMsgs.addAll(batchSendMsgs(asyncHigh, msgBatchCount, MessagePriority.HIGH));
//
//        oldConsumer.start();
//        Assert.assertTrue(latch.await(10, TimeUnit.SECONDS));
//
//        for (int i = 0; i < 2 * msgBatchCount; i++) {
//            assertEquals(receivedMsgs.get(i).getPriority(), MessagePriority.HIGH);
//        }
//        for (int i = 2 * msgBatchCount; i < 4 * msgBatchCount; i++) {
//            assertEquals(receivedMsgs.get(i).getPriority(), MessagePriority.MIDDLE);
//        }
//        for (int i = 4 * msgBatchCount; i < 6 * msgBatchCount; i++) {
//            assertEquals(receivedMsgs.get(i).getPriority(), MessagePriority.LOW);
//        }
//
//        assertMsgListEqual(sendMsgs, receivedMsgs);
//
//        oldConsumer.stop();
//    }
}
