package com.baojie.fbq;

import com.baojie.fbq.queue.FQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class TestQueue {

    public static void main(String args[]) throws Exception {
        //FQueue fQueue = new FQueue("/home/baojie/liuxin/source/fileblockqueue", 200);
        FQueue<Runnable> fQueue = new FQueue<>("/home/baojie/liuxin/source/fileblockqueue", 1024 * 1024 * 4,
                10000000);
        ThreadPoolExecutor pool = new ThreadPoolExecutor(4, 512, 180, TimeUnit.SECONDS, fQueue);

        pool.prestartAllCoreThreads();
        TimeUnit.SECONDS.sleep(15);


        for (int i=0;i<32 ;i++ ) {
            //Runer one = new Runer(i + "");
            //pool.submit(one);
        }

        TimeUnit.SECONDS.sleep(15);
        int i=32;
       // for (;; ) {
       //     Runer one = new Runer(i + "");
       //     fQueue.put(one);
       //     i++;
       // }

    }

    public static final class Runer implements Runnable {
        private static final Logger log = LoggerFactory.getLogger(Runer.class);
        private final String info;

        public Runer(String info) {
            this.info = info;
        }

        @Override
        public void run() {
            log.info(getInfo());
        }

        public String getInfo() {
            return info;
        }

        @Override
        public String toString() {
            return "Runer{" +
                    "info='" + info + '\'' +
                    '}';
        }
    }


}
