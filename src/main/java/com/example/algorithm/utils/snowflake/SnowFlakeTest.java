package com.example.algorithm.utils.snowflake;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class SnowFlakeTest {
    public static void main(String[] args) {
        Long start = System.currentTimeMillis();
        int num = 100 * 10000;
       //所有线程阻塞，然后统一开始
        CountDownLatch begin = new CountDownLatch(1);
        //主线程阻塞，直到所有分线程执行完毕
        CountDownLatch end = new CountDownLatch(num);

        try{
            ExecutorService executorService = Executors.newFixedThreadPool(2000);
            for (int i = 0; i < num; i++) {
                executorService.submit(new Runnable() {
                    @Override
                    public void run() {
                        try {
                            begin.await();
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }

                        Long id = SnowflakeIdFactory.getInstance().nextLongId();
                      //  String strId = SnowflakeIdFactory.getInstance().nextString("Test", 24);
                        //   System.out.println(id + "生成单个ID消耗毫秒数：" + time);

                        end.countDown();
                    }
                });
            }
            System.out.println("1秒后统一开始");
            Thread.sleep(1000);
            begin.countDown();
            executorService.shutdown();
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            try {
                end.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            Long time = System.currentTimeMillis() - start;
            System.out.println("***** longMap *******" + SnowflakeIdFactory.getInstance().longMap.size());
            System.out.println("***** stringMap ******" + SnowflakeIdFactory.getInstance().stringMap.size());
            System.out.println("**** 生成" + num + "条数据总消耗时间：" + time);
            SnowflakeIdFactory.getInstance().longMap.clear();
            SnowflakeIdFactory.getInstance().stringMap.clear();
        }

    }
}
