package com.baojie.fbq;

import com.baojie.fbq.queue.FQueue;
import junit.framework.TestCase;

import java.util.LinkedList;
import java.util.List;

/**
 * Unit test for simple App.
 */
public class AppTest extends TestCase {
    private static FQueue<byte[]> queue;

    static {
        try {
            queue = new FQueue<>("db", 1024 * 1024 * 4, Integer.MAX_VALUE);
            queue.clear();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    @Override
    protected void setUp() throws Exception {

    }

    @Override
    protected void tearDown() throws Exception {
        super.tearDown();
    }

    public void tesssstCrash() {
        queue.offer("testqueueoffer".getBytes());
        System.exit(9);
        try {
            Thread.sleep(100000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public void testOffer() {
        queue.offer("testqueueoffer".getBytes());
        assertEquals(new String(queue.poll()), "testqueueoffer");
    }

    public void testPoll() {
        queue.offer("testqueuepoll".getBytes());
        assertEquals(new String(queue.poll()), "testqueuepoll");
    }

    public void testAdd() {
        queue.add("testqueueadd".getBytes());
        assertEquals(new String(queue.poll()), "testqueueadd");
    }

    public void testAll() {
        queue.add("test1".getBytes());
        queue.add("test2".getBytes());
        assertEquals(new String(queue.poll()), "test1");
        queue.add("test3".getBytes());
        queue.add("test4".getBytes());
        assertEquals(new String(queue.poll()), "test2");
        assertEquals(new String(queue.poll()), "test3");
        System.out.println(new String(queue.poll()));
        StringBuffer sBuffer = new StringBuffer(1024);
        for (int i = 0; i < 1024; i++) {
            sBuffer.append("a");
        }
        String string = sBuffer.toString();
        assertEquals(0, queue.size());
        for (int i = 0; i < 100000; i++) {
            byte[] b = (string + i).getBytes();
            queue.offer(b);
        }
        assertEquals(100000, queue.size());
        for (int i = 0; i < 100000; i++) {
            if (i == 85301) {
                System.out.println(i);
            }
            byte[] b = queue.poll();
            if (b == null) {
                i--;
                System.out.println("null" + i);
                continue;
            }
            assertEquals(new String(b), (string + i));
        }
        queue.add("123".getBytes());
        queue.add("123".getBytes());
        assertEquals(queue.size(), 2);
        queue.clear();
        assertNull(queue.poll());
    }

    public void testFqueueVSList() {
        String message = "1234567890";
        byte[] bytes = message.getBytes();
        long start = System.currentTimeMillis();
        for (int i = 0; i < 1000000; i++) {
            queue.add(bytes);
        }
        System.out.println("Fqueue写入10字节10000000次:" + (System.currentTimeMillis() - start));
        queue.clear();
        List<byte[]> list = new LinkedList<byte[]>();
        start = System.currentTimeMillis();
        for (int i = 0; i < 1000000; i++) {
            list.add(bytes);
        }
        System.out.println("LinkedList写入10字节10000000次:" + (System.currentTimeMillis() - start));
    }

    public void testPerformance() {
        StringBuffer sBuffer = new StringBuffer(1024);
        for (int i = 0; i < 1024; i++) {
            sBuffer.append("a");
        }
        String string = sBuffer.toString();
        System.out.println("Test write 1000000 times 1K data to queue");
        long start = System.currentTimeMillis();
        for (int i = 0; i < 1000000; i++) {
            byte[] b = (string + i).getBytes();
            queue.offer(b);
        }
        System.out.println("spend time:" + (System.currentTimeMillis() - start) + "ms");
        System.out.println("Test read 1000000 times 1K data from queue");
        start = System.currentTimeMillis();
        for (int i = 0; i < 1000000; i++) {

            byte[] b = queue.poll();
            if (b == null) {
                i--;
                System.out.println("null" + i);
                continue;
            }
        }
        assertEquals(0, queue.size());
        System.out.println("spend:" + (System.currentTimeMillis() - start) + "ms");
    }

    public void testPerformance2() {
        StringBuffer sBuffer = new StringBuffer(1024);
        for (int i = 0; i < 10; i++) {
            sBuffer.append("a");
        }
        String string = sBuffer.toString();
        System.out.println("Test write 10000000 times 10 Bytes data to queue");
        long start = System.currentTimeMillis();
        for (int i = 0; i < 10000000; i++) {
            byte[] b = (string + i).getBytes();
            queue.offer(b);
        }
        System.out.println("spend time:" + (System.currentTimeMillis() - start) + "ms");
        System.out.println("Test read 10000000 times 10 bytes data from queue");
        start = System.currentTimeMillis();
        for (int i = 0; i < 10000000; i++) {
            byte[] b = queue.poll();
            if (b == null) {
                i--;
                System.out.println("null" + i);
                continue;
            }
        }
        assertEquals(0, queue.size());
        System.out.println("spend:" + (System.currentTimeMillis() - start) + "ms");
    }
}
