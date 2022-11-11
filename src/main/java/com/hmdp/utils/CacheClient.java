package com.hmdp.utils;

import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

/**
 * @author 6K
 * @date 2022/11/4 15:32
 */
@Slf4j
@Component
public class CacheClient {

    @Autowired
    private StringRedisTemplate stringRedisTemplate;

    public void set(String key, Object value, Long time, TimeUnit unit){
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(value),time,unit);
    }

    public void setWithLogicalExpire(String key, Object value, Long time, TimeUnit unit){
        RedisData redisData = new RedisData();
        redisData.setData(value);
        redisData.setExpireTime(LocalDateTime.now().plusSeconds(unit.toSeconds(time)));
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(redisData));
    }

    //解决缓存穿透工具类
    public <R,ID> R queryPassWithThrough(String keyPrefix, ID id, Class<R> type,
                                         Function<ID,R> dbFallBack, Long time, TimeUnit unit){
        //从Redis查询商铺缓存
        String json = stringRedisTemplate.opsForValue().get(keyPrefix + id);
        //缓存命中返回商铺信息
        /*
          字符串是否为非空白 空白的定义如下：
          1、不为null <br>
          2、不为不可见字符（如空格）
          3、不为""
         */
        if(StrUtil.isNotBlank(json)){
            return JSONUtil.toBean(json, type);

        }
        if(json != null){
            return null;
        }
        //未命中,到数据库查询
        R r = dbFallBack.apply(id);
        //数据库不存在返回错误信息
        if(r == null){
            //将空值写入Redis（解决缓存穿透）
            stringRedisTemplate.opsForValue().set(keyPrefix + id,"",time,unit);
            return null;
        }
        //数据库存在更新Redis并返回商铺信息
        this.set(keyPrefix + id,r,time,unit);
        return r;
    }

    private static final ExecutorService CACHE_REBUILD_EXECUTOR = Executors.newFixedThreadPool(10);

    //逻辑过期解决缓存击穿工具类
    public <R,ID> R queryLogicExpire(String keyPrefix,ID id,Class<R> type,
                                     Function<ID,R> dbFallBack,Long time, TimeUnit unit){
        //从Redis查询商铺缓存
        String json = stringRedisTemplate.opsForValue().get(keyPrefix + id);
        //缓存未命中
        if(StrUtil.isBlank(json)){
            return null;
        }
        //命中将Json反序列化为对象
        RedisData redisData = JSONUtil.toBean(json,RedisData.class);
        JSONObject data = (JSONObject) redisData.getData();
        R r = JSONUtil.toBean(data, type);
        LocalDateTime expireTime = redisData.getExpireTime();
        //判断过期时间
        if(expireTime.isAfter(LocalDateTime.now())){
            //未过期
            return r;
        }
        //缓存重建
        String key = keyPrefix + id;
        boolean isLock = tryLock(key);
        if(isLock){
            //开启独立线程，实行缓存重建
            CACHE_REBUILD_EXECUTOR.submit(() -> {
                //重建缓存
                try {
                    //查询数据库
                    R r1 = dbFallBack.apply(id);
                    //写入Redis
                    this.setWithLogicalExpire(key,r1,time,unit);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }finally {
                    //释放锁
                    unLock(key);
                }
            });
        }
        return r;
    }

    private boolean tryLock(String key){
        Boolean flag = stringRedisTemplate.opsForValue().setIfAbsent(key, "1", 10, TimeUnit.SECONDS);
        return BooleanUtil.isTrue(flag);
    }

    private void unLock(String key){
        stringRedisTemplate.delete(key);
    }
}
