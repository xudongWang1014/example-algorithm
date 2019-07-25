package com.example.algorithm.controller;


import com.example.algorithm.service.SimpleRedisLockService;
import com.example.algorithm.utils.redis.RedisUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;


@RestController
@RequestMapping("/test")
public class TestController {

    private static final Logger logger = LoggerFactory.getLogger(TestController.class);

    @Autowired
    private RedisUtil redisUtil;
    @Autowired
    private SimpleRedisLockService simpleRedisLockService;

    @RequestMapping(value = "/redis.do")
    public Object testDao(String redisKey) throws Exception {

        logger.info("===========test==============");

        redisUtil.setRedisStr(redisKey, "test redisUtil", 10);

        System.out.println(redisUtil.getRedisStrByKey(redisKey));

        return "redis测试成功";
    }

    @RequestMapping(value = "/acquire.do")
    public Object acquire(@RequestParam(required = false) String redisKey) throws Exception {

        boolean redisFlag = simpleRedisLockService.acquire(redisKey, 15*1000);

        String str = redisKey + "是否获取锁标记：" + redisFlag + "，value = " + redisUtil.getRedisStrByKey(redisKey);

        System.out.println(str);

//        boolean flag = simpleRedisLockService.release(redisKey);
//        String str1 = redisKey + "释放锁标记：" + flag;
//        System.out.println(str1);


        return str;
    }

    @RequestMapping(value = "/release.do")
    public Object release(@RequestParam(required = false) String redisKey) throws Exception {

        //释放锁
        boolean flag = simpleRedisLockService.releaseByOther(redisKey);

        String str = redisKey + "释放锁标记：" + flag;
        System.out.println(str);


        return redisKey + str;
    }
}


