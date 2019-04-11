package com.baojie.mfbq;

import org.apache.commons.lang3.JavaVersion;
import org.apache.commons.lang3.SystemUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class TestMFQueue {
    private static final Logger log = LoggerFactory.getLogger(TestMFQueue.class);
    static String s = "A";
    public static String test1Fun() {
        try {
            println("A");
            return s = "A";
        } finally {
            println("B");
            s = "B";
        }
    }
    public static Tmp test4Fun() {
        Tmp tmp = new Tmp("tmp");
        try {
            return tmp;
        } catch (Exception e) {
            println("catch block");
            return tmp;
        } finally {
            println("finally block");
            if (null != tmp) {
                tmp.setTest("test+tmp");
            }
            //return b;
        }
    }

    private static final class Tmp {
        private String test;

        public Tmp(String t) {
            this.test = t;
        }

        public String getTest() {
            return test;
        }

        public void setTest(String test) {
            this.test = test;
        }
    }

    private static void println(String s) {
        System.out.println(s);
    }

    public static void main(String args[]) throws Exception {

        String aaaa=test1Fun();

        Tmp test = test4Fun();

        String ver = SystemUtils.JAVA_VERSION;

        String ver0 = SystemUtils.JAVA_RUNTIME_NAME;
        String ver1 = SystemUtils.JAVA_RUNTIME_VERSION;
        String ver2 = SystemUtils.JAVA_SPECIFICATION_NAME;
        String ver3 = SystemUtils.JAVA_SPECIFICATION_VERSION;
        String ver4 = SystemUtils.JAVA_VM_INFO;

        String ver5 = SystemUtils.JAVA_VM_NAME;
        String ver6 = SystemUtils.JAVA_VM_SPECIFICATION_NAME;
        String ver7 = SystemUtils.JAVA_VM_SPECIFICATION_VERSION;
        String ver8 = SystemUtils.JAVA_VM_VERSION;

        boolean java_9 = SystemUtils.isJavaVersionAtLeast(JavaVersion.JAVA_9);


        final MFQueue mfq = new MFQueue("test_mf_queue", "/home/baojie/liuxin/source/fileblockqueue");

        Thread put = new Thread(new Runnable() {
            @Override
            public void run() {
                //for (int i = 0; i < 10000000; i++) {
                //    mfq.offer((i + "").getBytes());
                //}
            }
        });


        Thread take = new Thread(new Runnable() {
            @Override
            public void run() {
                for (; ; ) {
                    byte[] node = mfq.poll();
                    if (null != node) {
                        log.info(new String(node));
                    } else {
                        break;
                    }
                }
            }
        });

        put.start();

        take.start();


    }


}
