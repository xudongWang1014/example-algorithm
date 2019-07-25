package com.example.algorithm.utils.snowflake;

import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang3.RandomUtils;
import org.apache.commons.lang3.StringUtils;

import java.io.ObjectStreamException;
import java.io.Serializable;
import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.LockSupport;


/**
 * 分布式全局ID雪花算法解决方案
 *
 SnowFlake的结构如下(每部分用-分开):<br>
 * 0 - 0000000000 0000000000 0000000000 0000000000 0 - 00000 - 00000 - 000000000000 <br>
 * 1位标识，由于long基本类型在Java中是带符号的，最高位是符号位，正数是0，负数是1，所以id一般是正数，最高位是0<br>
 * 41位时间截(毫秒级)，注意，41位时间截不是存储当前时间的时间截，而是存储时间截的差值（当前时间截 - 开始时间截)
 * 得到的值），这里的的开始时间截，一般是我们的id生成器开始使用的时间，由我们程序来指定的（如下下面程序IdWorker类的startTime属性）。41位的时间截，可以使用69年，年T = (1L << 41) / (1000L * 60 * 60 * 24 * 365) = 69<br>
 * 10位的数据机器位，可以部署在1024个节点，包括5位datacenterId和5位workerId<br>
 * 12位序列，毫秒内的计数，12位的计数顺序号支持每个节点每毫秒(同一机器，同一时间截)产生4096个ID序号<br>
 * 加起来刚好64位，为一个Long型。<br>
 *
 *
 * 防止时钟回拨， 因为机器的原因会发生时间回拨，雪花算法是强依赖服务器时间的，如果时间发生回拨，
 * 有可能会生成重复的ID，这里可以对其进行优化,一般分为两个情况:
 * 如果时间回拨时间较短，比如配置5ms以内，那么可以直接等待一定的时间，让机器的时间追上来。
 * 如果时间的回拨时间较长，不能接受这么长的阻塞等待，有两个策略:
 * 1.直接拒绝，抛出异常，打日志，通知RD时钟回滚。
 * 2.利用扩展位，比如当这个时间回拨比较长的时候，我们可以不需要等待，直接在扩展位加1。2位的扩展位允许我们有3次大的时钟回拨，一般来说就够了，如果其超过三次选择抛出异常，打日志。
 * 通过上面的几种策略可以比较强的防护时钟回拨，防止出现回拨之后大量的异常出现。
 *
 * <pre>
 *     存在的问题：
 *     1.雪花算法产生的长整数的精度可能超过javascript能表达的精度，这会导致js获取的id与雪花算法算出来的id不一致。
 *        如雪花算法得到的是36594866121080832，  但是因为javascript丢失精度后只获取到36594866121080830， 这会导致对数据的所有操作都失效。
 *        解决办法：后端的语言获取到雪花算法的id后将其转换为String类型，这样js也会当做字符串来处理，就不会丢失精度了。
 *     2.依赖机器时钟，如果机器时钟回拨，会导致重复ID生成，暂时使用备用work和自增序列解决
 *     3..在单机上是递增的，但是由于设计到分布式环境，每台机器上的时钟不可能完全同步，有时候会出现不是全局递增的情况（此缺点可以认为无所谓，一般分布式ID只要求趋势递增，并不会严格要求递增～90%的需求都只要求趋势递增）
 * </pre>
 */
public class SnowflakeIdFactory implements Serializable {

    private static final long serialVersionUID = 7482921389735118270L;

    /**
     * 每台workerId服务器有3个备份workerId, 备份workerId数量越多, 可靠性越高, 但是可部署的sequence ID服务越少
     */
    private final long BACKUP_COUNT = 3;
    /**
     * worker id 的bit数，最多支持8192个节点
     */
    private static final long workerIdBits = 5L;
    /**
     * 数据中心标识位数
     */
    private static final long dataCenterIdBits = 5L;
    /**
     * 序列号，支持单节点最高每毫秒的最大ID数4096
     * 毫秒内自增位
     */
    private static final long sequenceBits = 12L;

    /**
     * 机器ID偏左移12位
     */
    private static final long workerIdShift = sequenceBits;

    /**
     * 数据中心ID左移17位(12+5)
     */
    private static final long dataCenterIdShift = sequenceBits + workerIdBits;

    /**
     * 时间毫秒左移22位(5+5+12)
     */
    private static final long timestampLeftShift = sequenceBits + workerIdBits + dataCenterIdBits;
    /**
     * sequence掩码，确保sequnce不会超出上限
     * 最大的序列号，4096
     * -1 的补码（二进制全1）右移12位, 然后取反
     * 生成序列的掩码，这里为4095 (0b111111111111=0xfff=4095)
     */
    private final long sequenceMask = -1L ^ (-1L << sequenceBits);

    /**
     * 原来代码 -1 的补码（二进制全1）右移13位, 然后取反
     */
    private static final long maxWorkerId = -1L ^ (-1L << workerIdBits);
    /**
     * 支持的最大数据标识id，结果是31
     */
    private static long maxDataCenterId = -1L ^ (-1L << dataCenterIdBits);
    /**
     * 进程编码
     */
    private long processId = 1L;
    /**
     * 工作机器ID(0~31)
     * snowflake算法给workerId预留了10位，即workId的取值范围为[0, 1023]，
     * 事实上实际生产环境不大可能需要部署1024个分布式ID服务，
     * 所以：将workerId取值范围缩小为[0, 511]，[512, 1023]
     * 这个范围的workerId当做备用workerId。workId为0的备用workerId是512，workId为1的备用workerId是513，以此类推
     */
    private static long workerId;
    /**
     * 数据中心ID(0~31)
     */
    private static long dataCenterId;
    /**
     * 当前毫秒生成的序列
     */
    private long sequence = 0L;
    /**
     * 上次生成ID的时间戳
     */
    private long lastTimestamp = -1L;
    /**
     * 保留workerId和lastTimestamp, 以及备用workerId和其对应的lastTimestamp
     */
    private final Map<Long, Long> workerIdLastTimeMap = new ConcurrentHashMap<>();
    /**
     * 最大容忍时间, 单位毫秒, 即如果时钟只是回拨了该变量指定的时间, 那么等待相应的时间即可; 考虑到sequence服务的高性能, 这个值不易过大
     */
    private final long MAX_BACKWARD_MS = 3;
    /**
     * EPOCH是服务器第一次上线时间点, 设置后不允许修改
     * 2018-08-08 08:08:08，从此时开始计算
     */
    private final long EPOCH = 1533686888000L;
    /**
     * 自增函数
     */
    private final LongAdder longAdder = new LongAdder();
    /**
     * 5位步长，超过后重置为0
     */
    private volatile Long maxlongAdder = 10000L;
    /**
     *  实例,volatile防止指令重排
     */
    private static volatile SnowflakeIdFactory INSTANCE;
    /**
     * 双重校验实现单例模式
     * @return
     */
    public static SnowflakeIdFactory getInstance() {
        // 第一次判断无需同步，如果 INSTANCE 已经被初始化，
        // 就直接返回，没有同步开销
        if (INSTANCE == null) {
            // 如果判断为空（多线程并发执行 getInstance，导致很多线程判断外层INSTANCE == NULL）
            synchronized (SnowflakeIdFactory.class) {
                // 进入同步后再判断一次，
                // 保证只有一个线程赋值给 INSTANCE，
                // 后续进来执行的线程都会判断 INSTANCE != NULL，不会再赋值
                if (INSTANCE == null) {
                    INSTANCE = new SnowflakeIdFactory(getWorkerId(dataCenterId, maxWorkerId), getDataCenterId(maxDataCenterId));
                }
            }
        }
        return INSTANCE;
    }
    /**
     * 因实现Serializable接口。反序列化单例需要 ,在反序列化时会被调用，若不加此方法 则反序列化的类不是单例的
     */
    private Object readResolve() throws ObjectStreamException {
        return getInstance();
    }
    /**
     * 构造函数
     * @param workerId 工作ID (0~31)
     * @param dataCenterId 数据中心ID (0~31)
     */
    private SnowflakeIdFactory(long workerId, long dataCenterId) {
        if (workerId > maxWorkerId || workerId < 0) {
            throw new IllegalArgumentException(String.format("worker Id can't be greater than %d or less than 0", maxWorkerId));
        }
        if (dataCenterId > maxDataCenterId || dataCenterId < 0) {
            throw new IllegalArgumentException(String.format("datacenter Id can't be greater than %d or less than 0", maxDataCenterId));
        }
        this.workerId = workerId;
        this.dataCenterId = dataCenterId;
        // 初始化workerId和其所有备份workerId与lastTimestamp
        // 假设workerId为0且BACKUP_AMOUNT为4, 那么map的值为: {0:0L, 256:0L, 512:0L, 768:0L}
        // 假设workerId为2且BACKUP_AMOUNT为4, 那么map的值为: {2:0L, 258:0L, 514:0L, 770:0L}
        for (int i = 0; i<= BACKUP_COUNT; i++){
            workerIdLastTimeMap.put(workerId + (i * maxWorkerId), 0L);
        }
    }

    /**
     * 获得下一个ID
     * <pre>
     *     1.该方法生成的ID长度为18-19
     * </pre>
     */
    public synchronized long nextLongId() {
        //生成ID
        Long tempId = this.buildNextId();
        return tempId;
    }

    /**
     * 获取下个ID， 尾部使用longAdder自增
     * @param prefix  前缀
     * @param length  不带前缀，后面数字保留的长度， 11 <= length <= 23
     * @return
     */
    public synchronized String nextString(String prefix, int length){

        Long curr = longAdder.longValue();
        if(curr.compareTo(0L) == 0 || curr.compareTo(maxlongAdder) > 0){
            //重置
            maxlongAdder = 1L;
            curr = 1L;
        }
        longAdder.increment();
        //生成ID
        Long id = buildNextId();
        //加上5位自增长
        String currId = String.format("%05d", curr);
        String tempId = id.toString() + currId;
        //截断处理
        if(length > 0 && length < tempId.length()){
            if(length < 11){
                // 自增5位 + nextId后6位
                length = 11;
            }
            tempId = tempId.substring(tempId.length() - length);
        }
        //添加前缀
        if(StringUtils.isNotBlank(prefix)){
            tempId = prefix + tempId;
        }

        return tempId;
    }
    /**
     * 在单节点上获得下一个ID
     * <pre>
     *     1.Long类型最大值为9223372036854775807，长度：19
     *     2.该方法生成的ID长度为18-19
     *     3.考虑时钟回拨缺陷: 如果连续两次时钟回拨, 可能还是会有问题, 但是这种概率极低极低
     * </pre>
     */
    private long buildNextId() {
        long currentTimestamp = timeGen();
        // 当发生时钟回拨时
        if (currentTimestamp < lastTimestamp){
            // 如果时钟回拨在可接受范围内, 等待即可
            long offset = lastTimestamp - currentTimestamp;
            if ( offset <= MAX_BACKWARD_MS){
                try {
                    //睡（lastTimestamp - currentTimestamp）ms让其追上
                    LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(offset));
                    currentTimestamp = timeGen();
                    //如果时间还小于当前时间，抛异常
                    if (currentTimestamp < lastTimestamp) {
                        //服务器时钟被调整了,ID生成器停止服务.
                        throw new RuntimeException(String.format("Clock moved backwards.  Refusing to generate id for %d milliseconds", lastTimestamp - currentTimestamp));
                    }
                } catch (Exception e) {
                    throw new RuntimeException("Thread sleep exception:" + e.getMessage(), e);
                }
            }else {
                //尝试在备份workID上生成
                tryGenerateKeyOnBackup(currentTimestamp);
            }
        }
        //对时钟回拨简单处理
       /* if (currentTimestamp < lastTimestamp) {
            //服务器时钟被调整了,ID生成器停止服务.
            throw new RuntimeException(String.format("Clock moved backwards.  Refusing to generate id for %d milliseconds", lastTimestamp - currentTimestamp));
        }*/

        // 如果和最后一次请求处于同一毫秒, 那么sequence+1
        if (lastTimestamp == currentTimestamp) {
            // 如果当前生成id的时间还是上次的时间，那么对sequence序列号进行+1
            sequence = (sequence + 1) & sequenceMask;
            //毫秒内序列溢出
            if (sequence == 0) {
                //自旋等待到下一毫秒
                currentTimestamp = waitUntilNextTime(lastTimestamp);
            }

        } else {
            // 如果是一个更近的时间戳, 那么sequence归零
            sequence = 0L;
        }
        // 更新上次生成id的时间戳
        lastTimestamp = currentTimestamp;
        // 更新map中保存的workerId对应的lastTime
        workerIdLastTimeMap.put(workerId, lastTimestamp);
        // 进行移位操作生成int64的唯一ID，时间戳右移动23位
        long timestamp = (currentTimestamp - EPOCH) << timestampLeftShift;

        //workerId 右移动10位
        long workerId = this.workerId << workerIdShift;

        //dataCenterId 右移动(sequenceBits + workerIdBits = 17位)
        long dataCenterId = this.dataCenterId << dataCenterIdShift;
        return timestamp | dataCenterId | workerId | sequence;
    }

    /**
     * 尝试在workerId的备份workerId上生成，BACKUP_COUNT即备份workerId数越多，
     * sequence服务避免时钟回拨影响的能力越强，但是可部署的sequence服务越少，
     * 设置BACKUP_COUNT为3，最多可以部署1024/(3+1)即256个sequence服务，完全够用，
     * 抗时钟回拨影响的能力也得到非常大的保障。
     * @param currentMillis 当前时间
     */
    private long tryGenerateKeyOnBackup(long currentMillis){
        // 遍历所有workerId(包括备用workerId, 查看哪些workerId可用)
        for (Map.Entry<Long, Long> entry:workerIdLastTimeMap.entrySet()){
            this.workerId = entry.getKey();
            // 取得备用workerId的lastTime
            Long tempLastTime = entry.getValue();
            lastTimestamp = tempLastTime == null ? 0L : tempLastTime;

            // 如果找到了合适的workerId
            if (lastTimestamp <= currentMillis){
                return lastTimestamp;
            }
        }

        // 如果所有workerId以及备用workerId都处于时钟回拨, 那么抛出异常
        throw new IllegalStateException("Clock is moving backwards, current time is " + currentMillis + " milliseconds, workerId map = " + JSONObject.toJSONString(workerIdLastTimeMap));
    }

    /**
     * 阻塞到下一个毫秒，直到获得新的时间戳
     * @param lastTimestamp 上次生成ID的时间截
     * @return 当前时间戳
     */
    protected long waitUntilNextTime(long lastTimestamp) {
        long timestamp = timeGen();
        while (timestamp <= lastTimestamp) {
            timestamp = timeGen();
        }
        return timestamp;
    }

    /**
     * 获取当前系统时间
     */
    protected long timeGen() {
        return System.currentTimeMillis();
    }

    /**
     *  获取WorkerId
     * @param dataCenterId
     * @param maxWorkerId
     * @return
     */
    protected static long getWorkerId(long dataCenterId, long maxWorkerId) {
        StringBuffer mpid = new StringBuffer();
        mpid.append(dataCenterId);
        String name = ManagementFactory.getRuntimeMXBean().getName();
        if (!name.isEmpty()) {
            // GET jvmPid
            mpid.append(name.split("@")[0]);
        }
        // MAC + PID 的 hashcode 获取16个低位
        return (mpid.toString().hashCode() & 0xffff) % (maxWorkerId + 1);
    }

    /**
     * 获取机器编码 用来做数据ID
     */
    protected static long getDataCenterId(long tempMaxDataCenterId) {
        if (tempMaxDataCenterId < 0L  || tempMaxDataCenterId > maxDataCenterId) {
            tempMaxDataCenterId = maxDataCenterId;
        }
        long id = 0L;
        try {
            InetAddress ip = InetAddress.getLocalHost();
            NetworkInterface network = NetworkInterface.getByInetAddress(ip);
            if (network == null) {
                id = 1L;
            } else {
                byte[] mac = network.getHardwareAddress();
                id = ((0x000000FF & (long) mac[mac.length - 1])
                        | (0x0000FF00 & (((long) mac[mac.length - 2]) << 8))) >> 6;
                id = id % (tempMaxDataCenterId + 1);
            }
        } catch (Exception e) {
            e.printStackTrace();
            // 如果获取失败，则使用随机数备用
            return RandomUtils.nextLong(0,31);
        }
        return id;
    }

}
