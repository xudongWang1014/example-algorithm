# example-algorithm
项目简介：各种小算法集合<br>
1.雪花算法，在原有的Twitter上做了如下改进：<br>
  1.1 增加时针回拨处理，使用备用work<br>
  1.2 针对毫秒内的并发，使用自增序列，能保证一定的尾缀有序<br>

2.基于redis的简单分布式锁<br>
  2.1 在不使用redisson自带的redLock情况，通过判断过期时间实现简单的分布式锁<br>
