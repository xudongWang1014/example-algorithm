package com.example.algorithm.impl;

import com.example.algorithm.service.SimpleRedisLockService;
import com.example.algorithm.utils.redis.RedisUtil;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

/**
 *  简单redis锁
 * <pre>
 *
 *     问：为什么要加过期时间？
 *     答：如果一个进程获得锁之后，断开了与redis的连接（进程挂断或者网络中断），那么锁一直的不断释放，其他的进程就一直获取不到锁，就出现了 “死锁”
 *
 *     问：setnx和expire(设置超时时间)不是原子操作，当setnx后，redis宕掉，则此锁无法释放掉
 *     答：使用带有原子操作的redis客户端或者在value里携带锁过期时间，其他进程通过检测value来判断是否已过期
 *
 *     问：锁超时时，不能简单地使用 DEL 命令释放锁？
 *     答：考虑以下情况，进程P1已经首先获得了锁 lock.foo，然后进程P1挂掉了。进程P2，P3正在不断地检测锁是否已释放或者已超时，执行流程如下:
 *        1 .P2和P3进程读取键 lock.foo 的值，检测锁是否已超时（通过比较当前时间和键 lock.foo 的值来判断是否超时）
 *        2. P2和P3进程发现锁 lock.foo 已超时
 *        3. P2执行 DEL lock.foo命令
 *        4. P2执行 SETNX lock.foo命令，并返回1，即P2获得锁
 *        5. P3执行 DEL lock.foo命令将P2刚刚设置的键 lock.foo 删除（这步是由于P3刚才已检测到锁已超时）
 *        6. P3执行 SETNX lock.foo命令，并返回1，即P3获得锁
 *        7. P2和P3同时获得了锁
 *
 *
 *     问：无法保证在集群环境下完全有效，redis主从是异步的，如果master挂掉，锁没有同步到slave，就可能会导致多个实例获取到锁。
 *     答：需更换redis客户端为redisson，采用其内置redlock算法来加锁，解决分布式锁缺陷
 *
 *     问：业务执行时间大于锁过期时间，其他线程发现锁过期后，获取到锁，然后执行自身业务后删除锁。 这样可能多个线程可以获取到锁。
 *     答：可以增加判断：在删除锁的时候，判断是否为本线程加的锁
 *
 *     缺陷：在并发极高的场景下，比如抢红包场景，可能存在UnixTimestamp重复问题，另外由于不能保证分布式环境下的物理时钟一致性，也可能存在UnixTimestamp重复问题。
 *     缺陷：毫秒内的时间判断没法处理
 * </pre>
 */
@Component
public class SimpleRedisLockServiceImpl implements SimpleRedisLockService {

    private final static Logger logger = LoggerFactory.getLogger(SimpleRedisLockServiceImpl.class);

    @Autowired
    private RedisUtil redisUtil;
    /**
     *  是否已经获取锁
     *  <pre>
     *      1.@Component默认为单例
     *      2.采用ThreadLocal处理并发
     *  </pre>
     */
    private ThreadLocal<Boolean> threadLocal = new ThreadLocal<>();

    /**
     * 生成锁对于的值
     */
    private String makeValue(int expireMills){
        long expires = System.currentTimeMillis() + expireMills + 1;
        String expiresStr = String.valueOf(expires); // 锁到期时间
        return expiresStr;
    }
    /**
     * 获取锁，立即返回，不会等待重复获取
     *
     * @param lockKey     锁的键值
     * @param expireMills 锁超时（ms）, 防止线程得到锁之后, 不去释放锁， 根据具体业务设置
     * @return
     */
    @Override
    public boolean acquire(String lockKey, int expireMills) {

        if(StringUtils.isBlank(lockKey) || expireMills < 0){
            return  false;
        }

        try {
            //设值和过期时间分开，如果服务挂掉，可能导致设了一个永久key
            if (redisUtil.setIfAbsent(lockKey, this.makeValue(expireMills), expireMills, TimeUnit.MILLISECONDS)) {
                logger.info("成功获取redis锁，对应key：" + lockKey);
                threadLocal.set(Boolean.TRUE);
                return true;
            }
            String currentValueStr = redisUtil.getRedisStrByKey(lockKey);// redis里的时间

            // 判断是否为空, 不为空的情况下, 如果被其他线程设置了值, 则第二个条件判断是过不去的
            if (currentValueStr != null && Long.parseLong(currentValueStr) < System.currentTimeMillis()) {

                //重新设值，并返回旧值
                String oldValueStr = redisUtil.getAndSet(lockKey, this.makeValue(expireMills));
                // 获取上一个锁到期时间, 并设置现在的锁到期时间
                // 只有一个线程才能获取上一个线程的设置时间
                // 如果这个时候, 多个线程恰好都到了这里, 但是只有一个线程的设置值和当前值相同, 它才有权利获取锁
                if (oldValueStr == null || oldValueStr.equals(currentValueStr)) {
                    // 1. 若为空，代表lockkey已经没有了（可能是已经被释放），所以可以正常拿到锁；
                    // 2. 若跟currentValueStr相等，则代表在两个菱形判断条件过程中，没有其他服务过来争取锁，而lockkey是已经处于超时的状况，因而也可以正常去获取锁
                    logger.info("成功获取redis锁，对应key：" + lockKey);
                    threadLocal.set(Boolean.TRUE);
                    return true;
                }
            }
        } catch (Exception e) {
            logger.error("锁" + lockKey + "获取异常：" + e.getMessage(), e);
        }

        return false;
    }

    /**
     * 悲观获取锁，会等待重复获取,每100ms重试一次
     *
     * @param lockKey      锁的键值
     * @param expireMills  锁超时（ms）, 防止线程得到锁之后, 不去释放锁， 根据具体业务设置
     * @param timeoutMills 锁等待（ms）, 防止线程饥饿， 根据具体业务设置
     * @return
     */
    @Override
    public boolean acquire(String lockKey, int expireMills, int timeoutMills) {

        if(StringUtils.isBlank(lockKey) || expireMills < 0  || timeoutMills < 0){
            return  false;
        }

        int timeout = timeoutMills;
        try {

            while (timeout >= 0) {
                //设值和过期时间分开，如果服务挂掉，可能导致设了一个永久key
                if (redisUtil.setIfAbsent(lockKey, this.makeValue(expireMills), expireMills, TimeUnit.MILLISECONDS)) {
                    logger.info("成功获取redis锁，对应key：" + lockKey);
                    threadLocal.set(Boolean.TRUE);
                    return true;
                }
                String currentValueStr = redisUtil.getRedisStrByKey(lockKey);// redis里的时间

                // 判断是否为空, 不为空的情况下, 如果被其他线程设置了值, 则第二个条件判断是过不去的
                if (currentValueStr != null && Long.parseLong(currentValueStr) < System.currentTimeMillis()) {

                    //重新设值，并返回旧值
                    String oldValueStr = redisUtil.getAndSet(lockKey, this.makeValue(expireMills));

                    // 获取上一个锁到期时间, 并设置现在的锁到期时间
                    // 只有一个线程才能获取上一个线程的设置时间
                    // 如果这个时候, 多个线程恰好都到了这里, 但是只有一个线程的设置值和当前值相同, 它才有权利获取锁
                    if (oldValueStr == null || oldValueStr.equals(currentValueStr)) {
                        // 1. 若为空，代表lockkey已经没有了（可能是已经被释放），所以可以正常拿到锁；
                        // 2. 若跟currentValueStr相等，则代表在两个菱形判断条件过程中，没有其他服务过来争取锁，而lockkey是已经处于超时的状况，因而也可以正常去获取锁
                        logger.info("成功获取redis锁，对应key：" + lockKey);
                        threadLocal.set(Boolean.TRUE);
                        return true;
                    }
                }

                timeout -= 100;
                // 等待timeout毫秒
                LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(timeout));
            }
        } catch (Exception e) {
            logger.error("锁" + lockKey + "获取异常：" + e.getMessage(), e);
        }

        return false;
    }

    /**
     * 同一个线程释放锁
     *
     * @param lockKey
     */
    @Override
    public boolean release(String lockKey) {
        boolean release = false;
        try {

            Boolean flag = threadLocal.get();

            if (Boolean.TRUE.equals(flag) && StringUtils.isNotBlank(lockKey)) {

                String currentValueStr = redisUtil.getRedisStrByKey(lockKey);// redis里的时间

                // 校验是否超过有效期, 如果不在有效期内, 那说明当前锁已超时，可能已由其他进程获得, 不能进行删除锁操作
                if (currentValueStr != null && Long.parseLong(currentValueStr) > System.currentTimeMillis()) {
                    redisUtil.deleteKey(lockKey);
                    threadLocal.remove();
                    logger.info("成功释放redis锁，对应key：" + lockKey);
                    release = true;
                }
            }
        } catch (Exception e) {
            logger.error("锁" + lockKey + "释放出现异常：" + e.getMessage(), e);
        }

        return release;
    }

    /**
     * 其他线程释放锁
     *
     * @param lockKey
     */
    @Override
    public boolean releaseByOther(String lockKey) {
        boolean release = false;
        try {

            if (StringUtils.isNotBlank(lockKey)) {

                String currentValueStr = redisUtil.getRedisStrByKey(lockKey);// redis里的时间

                // 校验是否超过有效期, 如果不在有效期内, 那说明当前锁已超时，可能已由其他进程获得, 不能进行删除锁操作
                if (currentValueStr != null && Long.parseLong(currentValueStr) > System.currentTimeMillis()) {
                    redisUtil.deleteKey(lockKey);
                    threadLocal.remove();
                    logger.info("成功释放redis锁，对应key：" + lockKey);
                    release = true;
                }
            }
        } catch (Exception e) {
            logger.error("锁" + lockKey + "释放出现异常：" + e.getMessage(), e);
        }

        return release;
    }
}
