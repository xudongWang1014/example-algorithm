package com.example.algorithm.service;

/**
 * 分布式锁接口
 */
public interface SimpleRedisLockService {
    /**
     *  获取锁，立即返回，不会等待重复获取
     * @param lockKey 锁的键值
     * @param expireMills 锁超时（ms）, 防止线程得到锁之后, 不去释放锁， 根据具体业务设置
     * @return
     */
    boolean acquire(String lockKey, int expireMills);

    /**
     *  悲观获取锁，会等待重复获取
     * @param lockKey 锁的键值
     * @param expireMills 锁超时（ms）, 防止线程得到锁之后, 不去释放锁， 根据具体业务设置
     * @param timeoutMills 锁等待（ms）, 防止线程饥饿， 根据具体业务设置
     * @return
     */
    boolean acquire(String lockKey, int expireMills, int timeoutMills);
    /**
     * 同一个线程释放锁
     * */
    boolean release(String lockKey);

    /**
     * 其他线程释放锁
     * */
    boolean releaseByOther(String lockKey);
}
