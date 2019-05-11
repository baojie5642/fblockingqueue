package com.baojie.fbq;

import com.baojie.fbq.bytes.LocalBytes;

public class TestQueue {

    public static void main(String args[]) throws Exception {
        //FQueue fQueue = new FQueue("/home/baojie/liuxin/source/fileblockqueue", 200);
        FQueue fQueue = new FQueue("/home/baojie/liuxin/source/fileblockqueue", 20);

        for (int i = 0; i < 20; i++) {
            byte[] one = LocalBytes.seria(new Runer(i + ""));
            fQueue.offer(one);
        }
        int i = 0;
        for (; ; ) {
            byte[] one = fQueue.poll();
            i++;
            if (null != one) {
                Runer runer = LocalBytes.deseria(one, Runer.class);
                System.out.println(runer.getInfo());
                runer.run();
            } else {
                if (i >= 20) {
                    break;
                }
            }
        }
        // System.out.println(i);
        fQueue.close();

    }

    public static final class Runer implements Runnable {

        private final String info;

        public Runer(String info) {
            this.info = info;
        }

        @Override
        public void run() {
            System.out.println(getInfo());
        }

        public String getInfo() {
            return info;
        }


    }


}
