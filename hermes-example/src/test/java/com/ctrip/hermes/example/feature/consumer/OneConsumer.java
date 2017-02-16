package com.ctrip.hermes.example.feature.consumer;

import org.unidal.lookup.ComponentTestCase;

public class OneConsumer extends ComponentTestCase {

//    private final int msgBatchCount = 100;
//    MQFactory factory;
//
//    @Before
//    public void init() throws ComponentLookupException {
//        factory = getContainer().lookup(MQFactory.class);
//    }
//
//    @Test
//    public void consumeOneMsg() throws ComponentLookupException, InterruptedException {
//        final String msgContent = "syncSendOneMsg: Message One.";
//        final Message oneMsg = new Message(msgContent);
//
//        final CountDownLatch latch = new CountDownLatch(2);
//        OldConsumer oldConsumer = buildConsumer(factory);
//        oldConsumer.setMessageListener(new MessageListener() {
//            @Override
//            public void onMessage(Message msg) {
//                assertMsgEqual(msg, oneMsg);
//                latch.countDown();
//            }
//        });
//        oldConsumer.start();
//
//        OldProducer syncOldProducer = buildSyncProducer(factory);
//        syncOldProducer.send(oneMsg);
//
//        OldProducer asyncOldProducer = buildAsyncProducer(factory);
//        asyncOldProducer.send(oneMsg);
//
//        assertTrue(latch.await(1, TimeUnit.DAYS));
//        oldConsumer.stop();
//    }
//
//    @Test
//    public void consumeManyMsgs() throws ComponentLookupException, InterruptedException {
//        Set<Message> sendMsgs = new HashSet<>(msgBatchCount);
//        final Set<Message> receiveMsgs = new HashSet<>(msgBatchCount);
//        final CountDownLatch latch = new CountDownLatch(2 * msgBatchCount);
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
//        OldProducer asyncOldProducer = buildAsyncProducer(factory);
//        sendMsgs.addAll(batchSendMsgs(asyncOldProducer, msgBatchCount));
//
//        OldProducer syncOldProducer = buildSyncProducer(factory);
//        sendMsgs.addAll(batchSendMsgs(syncOldProducer, msgBatchCount));
//
//        assertTrue(latch.await(10, TimeUnit.SECONDS));
//        assertMsgSetEqual(sendMsgs, receiveMsgs);
//
//        oldConsumer.stop();
//    }
//
//    /**
//     * 根据优先级消费，高优先级的message先消费
//     * 类似OneProducer.syncHighPriorityFirstReceived()，故省略。
//     */
//    @Test
//    public void consumeByPriority() {
//    }
//
//    /**
//     * 重新启动后，从启动时开始消费；
//     */
//    @Test
//    public void consumeFromStartup() throws InterruptedException {
//        final List<Message> receivedMsgs = new ArrayList<>();
//        List<Message> laterSentMsgs = new ArrayList<>();
//        OldConsumer oldConsumer = buildConsumer(factory);
//        oldConsumer.setMessageListener(new MessageListener() {
//            @Override
//            public void onMessage(Message msg) {
//                receivedMsgs.add(msg);
//            }
//        });
//        oldConsumer.start();
//        oldConsumer.stop();
//
//        batchSendMsgs(buildSyncProducer(factory), msgBatchCount);
//        batchSendMsgs(buildAsyncProducer(factory), msgBatchCount);
//
//        oldConsumer.start();
//
//        laterSentMsgs.addAll(batchSendMsgs(buildSyncProducer(factory), msgBatchCount));
//        laterSentMsgs.addAll(batchSendMsgs(buildAsyncProducer(factory), msgBatchCount));
//
//        Thread.sleep(2000);
//        assertMsgListEqual(laterSentMsgs, receivedMsgs);
//    }
//
//    /**
//     * 重新启动后，从注册的时间开始消费
//     */
//    @Test
//    public void consumeFromHead() throws InterruptedException {
//        final List<Message> receivedMsgs = new ArrayList<>();
//        List<Message> allSentMsgs = new ArrayList<>();
//        OldConsumer oldConsumer = buildGroupConsumer(factory, "SomeGroup");
//        oldConsumer.setMessageListener(new MessageListener() {
//            @Override
//            public void onMessage(Message msg) {
//                receivedMsgs.add(msg);
//            }
//        });
//        oldConsumer.start();
//        oldConsumer.stop();
//
//        allSentMsgs.addAll(batchSendMsgs(buildSyncProducer(factory), msgBatchCount));
//        allSentMsgs.addAll(batchSendMsgs(buildAsyncProducer(factory), msgBatchCount));
//
//        oldConsumer.start();
//
//        allSentMsgs.addAll(batchSendMsgs(buildSyncProducer(factory), msgBatchCount));
//        allSentMsgs.addAll(batchSendMsgs(buildAsyncProducer(factory), msgBatchCount));
//
//        Thread.sleep(2000);
//        assertMsgListEqual(allSentMsgs, receivedMsgs);
//    }
//
//    /**
//     * nack & resend
//     */
//    @Test
//    public void nackAndResend() throws ComponentLookupException, InterruptedException {
//        final Set<Message> oddReceived = new HashSet<>();
//        final Set<Message> evenReceived = new HashSet<>();
//        final CountDownLatch oddLatch = new CountDownLatch(50);
//        final CountDownLatch evenLatch = new CountDownLatch(50);
//
//        OldConsumer oldConsumer = buildConsumer(factory);
//        oldConsumer.setMessageListener(new MessageListener() {
//            @Override
//            public void onMessage(Message msg) {
//                if (Integer.valueOf(msg.getContent()) % 2 == 0) {
//                    evenReceived.add(msg);
//                    evenLatch.countDown();
//                } else {
//                    msg.nack();
//                }
//            }
//        });
//        oldConsumer.start();
//
//        OldProducer oldProducer = buildSyncProducer(factory);
//
//        for (int i = 0; i < 100; i++) {
//            oldProducer.send(new Message(String.valueOf(i)));
//        }
//
//        assertTrue(evenLatch.await(5, TimeUnit.SECONDS));
//        oldConsumer.stop();
//
//        oldConsumer.setMessageListener(new MessageListener() {
//            @Override
//            public void onMessage(Message msg) {
//                oddReceived.add(msg);
//                oddLatch.countDown();
//            }
//        });
//        oldConsumer.start();
//
//        assertTrue(oddLatch.await(5, TimeUnit.SECONDS));
//        assertEquals(evenReceived.size(), 50);
//        assertEquals(oddReceived.size(), 50);
//
//        for (Message message : evenReceived) {
//            assertTrue(Integer.valueOf(message.getContent()) % 2 == 0);
//        }
//
//        for (Message message : oddReceived) {
//            assertTrue(Integer.valueOf(message.getContent()) % 2 != 0);
//        }
//        oldConsumer.stop();
//    }
//
//    /**
//     * Consume by filter todo: to be implemented
//     */
//    @Test
//    public void consumeByFilter() {
//    }
//
//    /**
//     * 重新消费近段时间的消息 todo: to be implemented. 场景：OPS调整后，重新消费之前时间的
//     */
//    @Test
//    public void reConsumeByOffset() {
//    }
//
//    @Test
//    public void testNackTwice() throws Exception {
//        final Consumer c = buildGroupConsumer(factory, "cid");
//        Producer p = buildSyncProducer(factory);
//        final CountDownLatch latch = new CountDownLatch(3);
//        final AtomicInteger rcvCnt = new AtomicInteger(0);
//
//        c.setMessageListener(new MessageListener() {
//
//            @Override
//            public void onMessage(Message msg) {
//                if (rcvCnt.incrementAndGet() < 3) {
//                    msg.nack();
//                } else {
//                    c.stop();
//                }
//                latch.countDown();
//            }
//        });
//        c.start();
//
//        p.send(new Message(UUID.randomUUID().toString()));
//
//        assertTrue(latch.await(1, TimeUnit.DAYS));
//
//        final CountDownLatch latch2 = new CountDownLatch(1);
//        Consumer c2 = buildGroupConsumer(factory, "cid");
//        c2.setMessageListener(new MessageListener() {
//
//            @Override
//            public void onMessage(Message msg) {
//                latch2.countDown();
//            }
//        });
//        c2.start();
//
//        assertFalse(latch2.await(2, TimeUnit.SECONDS));
//    }

}
