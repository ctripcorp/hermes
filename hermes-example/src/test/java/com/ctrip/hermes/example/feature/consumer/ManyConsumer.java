package com.ctrip.hermes.example.feature.consumer;

import org.unidal.lookup.ComponentTestCase;

public class ManyConsumer extends ComponentTestCase {
//    private static final int consumerInGroupCount = 5;
//    private static final int groupCount = 5;
//    private static final int msgBatchCount = 100;
//
//    MQFactory factory;
//
//    @Before
//    public void init() throws ComponentLookupException {
//        factory = getContainer().lookup(MQFactory.class);
//    }
//
//    /**
//     * 若干Consumer作为一个Group消费一个Topic，一条消息只会到达其中一个。
//     */
//    @Test
//    public void consumerGroup() throws ComponentLookupException, InterruptedException {
//        List<OldConsumer> oldConsumerList = new ArrayList<>();
//        final List<Message> receivedMsgs = new CopyOnWriteArrayList<>();
//        final List<Message> sentMsgs = new ArrayList<>();
//
//        for (int i = 0; i < consumerInGroupCount; i++) {
//            OldConsumer oldConsumer = buildGroupConsumer(factory, "Test-Group");
//            oldConsumer.setMessageListener(new MessageListener() {
//                @Override
//                public void onMessage(Message msg) {
//                    receivedMsgs.add(msg);
//                }
//            });
//            oldConsumer.start();
//            oldConsumerList.add(oldConsumer);
//        }
//
//        OldProducer syncOldProducer = buildSyncProducer(factory);
//        OldProducer asyncOldProducer = buildAsyncProducer(factory);
//
//        sentMsgs.addAll(batchSendMsgs(syncOldProducer, msgBatchCount));
//        sentMsgs.addAll(batchSendMsgs(asyncOldProducer, msgBatchCount));
//
//        Thread.sleep(2000); //可能会多收，因此未用CountDownLatch
//        assertMsgListEqual(sentMsgs, receivedMsgs);
//
//        for (OldConsumer oldConsumer : oldConsumerList) {
//            oldConsumer.stop();
//        }
//    }
//
//    /**
//     * 多个Consumer,未设置groupID时默认是非持久的消费，应相互独立接收
//     */
//    @Test
//    public void manyConsumerOneWithoutGroup() throws InterruptedException {
//        final List<Message> msgR1 = new ArrayList<>();
//        final List<Message> msgR2 = new ArrayList<>();
//        List<Message> sentMsgs = new ArrayList<>();
//
//        OldConsumer oldConsumer1 = buildGroupConsumer(factory, "1");
////        Consumer consumer1 = buildConsumer(factory);
//        oldConsumer1.setMessageListener(new MessageListener() {
//            @Override
//            public void onMessage(Message msg) {
//                msgR1.add(msg);
//            }
//        });
//        oldConsumer1.start();
//
//        OldConsumer oldConsumer2 = buildConsumer(factory);
//        oldConsumer2.setMessageListener(new MessageListener() {
//            @Override
//            public void onMessage(Message msg) {
//                msgR2.add(msg);
//            }
//        });
//        oldConsumer2.start();
//
//        // send
//        OldProducer syncOldProducer = buildSyncProducer(factory);
//        OldProducer asyncOldProducer = buildAsyncProducer(factory);
//
//        sentMsgs.addAll(batchSendMsgs(syncOldProducer, msgBatchCount));
//        sentMsgs.addAll(batchSendMsgs(asyncOldProducer, msgBatchCount));
//
//
//        Thread.sleep(2000);
//        assertMsgListEqual(sentMsgs, msgR1);
//        assertMsgListEqual(sentMsgs, msgR2);
//
//        oldConsumer1.stop();
//        oldConsumer2.stop();
//    }
//
//    /**
//     * 两个Consumer都不设置group，应接受相互独立。
//     */
//    @Test
//    public void manyConsumerAllWithoutGroup() throws InterruptedException {
//        final List<Message> msgR1 = new ArrayList<>();
//        final List<Message> msgR2 = new ArrayList<>();
//        List<Message> sentMsgs = new ArrayList<>();
//
//        OldConsumer oldConsumer1 = buildConsumer(factory);
//        oldConsumer1.setMessageListener(new MessageListener() {
//            @Override
//            public void onMessage(Message msg) {
//                msgR1.add(msg);
//            }
//        });
//        oldConsumer1.start();
//
//        OldConsumer oldConsumer2 = buildConsumer(factory);
//        oldConsumer2.setMessageListener(new MessageListener() {
//            @Override
//            public void onMessage(Message msg) {
//                msgR2.add(msg);
//            }
//        });
//        oldConsumer2.start();
//
//        // send
//        OldProducer syncOldProducer = buildSyncProducer(factory);
//        OldProducer asyncOldProducer = buildAsyncProducer(factory);
//
//        sentMsgs.addAll(batchSendMsgs(syncOldProducer, msgBatchCount));
//        sentMsgs.addAll(batchSendMsgs(asyncOldProducer, msgBatchCount));
//
//        Thread.sleep(2000);
//        assertMsgListEqual(sentMsgs, msgR1);
//        assertMsgListEqual(sentMsgs, msgR2);
//
//        oldConsumer1.stop();
//        oldConsumer2.stop();
//    }
//
//    /**
//     * 多个设了GroupID的Consumer，不同的group接收相互独立。
//     */
//    @Test
//    public void manyConsumerWithGroup() throws InterruptedException {
//        final List<Message> msgR1 = new ArrayList<>();
//        final List<Message> msgR2 = new ArrayList<>();
//        List<Message> sentMsgs = new ArrayList<>();
//
//        OldConsumer oldConsumer1 = buildGroupConsumer(factory, "1");
////        Consumer consumer1 = buildConsumer(factory);
//        oldConsumer1.setMessageListener(new MessageListener() {
//            @Override
//            public void onMessage(Message msg) {
//                msgR1.add(msg);
//            }
//        });
//        oldConsumer1.start();
//
//        OldConsumer oldConsumer2 = buildConsumer(factory);
//        oldConsumer2.setMessageListener(new MessageListener() {
//            @Override
//            public void onMessage(Message msg) {
//                msgR2.add(msg);
//            }
//        });
//        oldConsumer2.start();
//
//        // send
//        OldProducer syncOldProducer = buildSyncProducer(factory);
//        OldProducer asyncOldProducer = buildAsyncProducer(factory);
//
//        sentMsgs.addAll(batchSendMsgs(syncOldProducer, msgBatchCount));
//        sentMsgs.addAll(batchSendMsgs(asyncOldProducer, msgBatchCount));
//
//        Thread.sleep(2000);
//        assertMsgListEqual(sentMsgs, msgR1);
//        assertMsgListEqual(sentMsgs, msgR2);
//
//        oldConsumer1.stop();
//        oldConsumer2.stop();
//    }
//
//    /**
//     * 若干独立Consumer与若干ConsumerGroup消息同一Topic。
//     * 每条消息会到达独立Consumer，且只会到达每个ConsumerGroup其中一个Consumer.
//     */
//    @Test
//    public void consumeSameTopic() throws ComponentLookupException, InterruptedException {
//        List<Message> sentMsgs = new ArrayList<>();
//
//        // build List<ConsumerGroup>
//        Map<List<OldConsumer>, List<Message>> groupList = new HashMap<>();
//
//        for (int number = 0; number < groupCount; number++) {
//            final List<OldConsumer> group = new ArrayList<>();
//            final List<Message> receivedMsgs = new CopyOnWriteArrayList<>();
//            String groupId = "Group--" + number;
//            for (int i = 0; i < consumerInGroupCount; i++) {
//                OldConsumer oldConsumer = buildGroupConsumer(factory, groupId);
//                oldConsumer.setMessageListener(new MessageListener() {
//                    @Override
//                    public void onMessage(Message msg) {
//                        receivedMsgs.add(msg);
//                    }
//                });
//                oldConsumer.start();
//                group.add(oldConsumer);
//            }
//
//            groupList.put(group, receivedMsgs);
//        }
//
//        // build single consumers
//        final List<Message> msgForConsumer1 = new ArrayList<>();
//        OldConsumer oldConsumer1 = buildConsumer(factory);
//        oldConsumer1.setMessageListener(new MessageListener() {
//            @Override
//            public void onMessage(Message msg) {
//                msgForConsumer1.add(msg);
//            }
//        });
//        oldConsumer1.start();
//
//        final List<Message> msgForConsumer2 = new ArrayList<>();
//        OldConsumer oldConsumer2 = buildConsumer(factory);
//        oldConsumer2.setMessageListener(new MessageListener() {
//            @Override
//            public void onMessage(Message msg) {
//                msgForConsumer2.add(msg);
//            }
//        });
//        oldConsumer2.start();
//
//        // send msgs
//        OldProducer syncOldProducer = buildSyncProducer(factory);
//        OldProducer asyncOldProducer = buildAsyncProducer(factory);
//        sentMsgs.addAll(batchSendMsgs(syncOldProducer, msgBatchCount));
//        sentMsgs.addAll(batchSendMsgs(asyncOldProducer, msgBatchCount));
//
//        Thread.sleep(3000);
//
//        assertMsgListEqual(sentMsgs, msgForConsumer1);
//        assertMsgListEqual(sentMsgs, msgForConsumer2);
//
//        for (List<Message> messages : groupList.values()) {
//            assertMsgListEqual(sentMsgs, messages);
//        }
//
//        //stop all consumers.
//        for (List<OldConsumer> oldConsumers : groupList.keySet()) {
//            for (OldConsumer oldConsumer : oldConsumers) {
//                oldConsumer.stop();
//            }
//        }
//        oldConsumer1.stop();
//        oldConsumer2.stop();
//    }
}
