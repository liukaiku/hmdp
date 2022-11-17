package com.hmdp.service.impl;

import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.hmdp.dto.Result;
import com.hmdp.entity.Shop;
import com.hmdp.mapper.ShopMapper;
import com.hmdp.service.IShopService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.CacheClient;
import com.hmdp.utils.RedisConstants;
import com.hmdp.utils.RedisData;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static com.hmdp.utils.RedisConstants.*;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class ShopServiceImpl extends ServiceImpl<ShopMapper, Shop> implements IShopService {

    @Autowired
    private StringRedisTemplate stringRedisTemplate;

    @Autowired
    private CacheClient cacheClient;


    @Override
    public Result queryById(Long id) {
        //缓存穿透
        //Shop shop = queryPassWithThrough(id);
//        Shop shop = cacheClient
//                .queryPassWithThrough(CACHE_SHOP_KEY, id, Shop.class, this::getById, CACHE_SHOP_TTL, TimeUnit.MINUTES);

        //用互斥锁解决缓存击穿
        //Shop shop = queryWithMutex(id);

        //逻辑过期解决缓存击穿
        Shop shop = cacheClient.queryLogicExpire(CACHE_SHOP_KEY, id, Shop.class,
                this::getById, CACHE_SHOP_TTL, TimeUnit.MINUTES);
        if (shop == null) {
            return Result.fail("店铺不存在");
        }
        return Result.ok(shop);
    }

//    private static final ExecutorService CACHE_REBUILD_EXECUTOR = Executors.newFixedThreadPool(10);
//
//    public Shop queryLogicExpire(Long id){
//        //从Redis查询商铺缓存
//        String shopString = stringRedisTemplate.opsForValue().get(CACHE_SHOP_KEY + id);
//        //缓存未命中
//        if(StrUtil.isBlank(shopString)){
//            return null;
//        }
//        //命中将Json反序列化为对象
//        RedisData redisData = JSONUtil.toBean(shopString, RedisData.class);
//        JSONObject data = (JSONObject) redisData.getData();
//        Shop shop = JSONUtil.toBean(data, Shop.class);
//        LocalDateTime expireTime = redisData.getExpireTime();
//        //判断过期时间
//        if(expireTime.isAfter(LocalDateTime.now())){
//            //未过期
//            return shop;
//        }
//        //缓存重建
//        String key = LOCK_SHOP_KEY + id;
//        boolean isLock = tryLock(key);
//        if(isLock){
//            //开启独立线程，实行缓存重建
//            CACHE_REBUILD_EXECUTOR.submit(() -> {
//                //重建缓存
//                try {
//                    saveShop2Redis(id,20L);
//                } catch (Exception e) {
//                    throw new RuntimeException(e);
//                }finally {
//                    //释放锁
//                    unLock(key);
//                }
//            });
//        }
//        return shop;
//    }

//    public Shop queryWithMutex(Long id){
//        //从Redis查询商铺缓存
//        String shopString = stringRedisTemplate.opsForValue().get(CACHE_SHOP_KEY + id);
//        //缓存命中返回商铺信息
//        if(StrUtil.isNotBlank(shopString)){
//            return JSONUtil.toBean(shopString, Shop.class);
//
//        }
//        if(shopString != null){
//            return null;
//        }
//        //实现缓存重建
//        //1. 获取互斥锁
//        String key = LOCK_SHOP_KEY + id;
//        Shop shop;
//        try {
//            boolean isLock = tryLock(key);
//            //2. 失败，休眠并重试
//            if(!isLock){
//                Thread.sleep(50);
//                queryWithMutex(id);
//            }
//            //3. 成功，根据id查询数据库
//            shop = getById(id);
//            //数据库不存在返回错误信息
//            if(shop == null){
//                //将空值写入Redis（解决缓存穿透）
//                stringRedisTemplate.opsForValue().set(CACHE_SHOP_KEY + id,"",CACHE_NULL_TTL,TimeUnit.MINUTES);
//                return null;
//            }
//            //数据库存在更新Redis并返回商铺信息
//            stringRedisTemplate.opsForValue().set(CACHE_SHOP_KEY + id,JSONUtil.toJsonStr(shop)
//                    ,CACHE_SHOP_TTL, TimeUnit.MINUTES);
//        } catch (InterruptedException e) {
//            throw new RuntimeException(e);
//        }finally {
//            //4. 释放互斥锁
//            unLock(key);
//        }
//        return shop;
//    }

    /**
     * 解决缓存穿透逻辑
     */
//    public Shop queryPassWithThrough(Long id){
//        //从Redis查询商铺缓存
//        String shopString = stringRedisTemplate.opsForValue().get(CACHE_SHOP_KEY + id);
//        //缓存命中返回商铺信息
//        /*
//          字符串是否为非空白 空白的定义如下：
//          1、不为null <br>
//          2、不为不可见字符（如空格）
//          3、不为""
//         */
//        if(StrUtil.isNotBlank(shopString)){
//            return JSONUtil.toBean(shopString, Shop.class);
//
//        }
//        if(shopString != null){
//            return null;
//        }
//        //未命中,到数据库查询
//        Shop shop = getById(id);
//        //数据库不存在返回错误信息
//        if(shop == null){
//            //将空值写入Redis（解决缓存穿透）
//            stringRedisTemplate.opsForValue().set(CACHE_SHOP_KEY + id,"",CACHE_NULL_TTL,TimeUnit.MINUTES);
//            return null;
//        }
//        //数据库存在更新Redis并返回商铺信息
//        stringRedisTemplate.opsForValue().set(CACHE_SHOP_KEY + id,JSONUtil.toJsonStr(shop)
//                ,CACHE_SHOP_TTL, TimeUnit.MINUTES);
//        return shop;
//    }

    @Override
    @Transactional
    public Result update(Shop shop) {
        Long id = shop.getId();
        if(id == null){
            return Result.fail("商铺id不能为空");
        }
        //更新数据库
        updateById(shop);
        //删除缓存
        stringRedisTemplate.delete(CACHE_SHOP_KEY + id);
        return Result.ok();
    }

//    private boolean tryLock(String key){
//        Boolean flag = stringRedisTemplate.opsForValue().setIfAbsent(key, "1", 10, TimeUnit.SECONDS);
//        return BooleanUtil.isTrue(flag);
//    }
//
//    private void unLock(String key){
//        stringRedisTemplate.delete(key);
//    }

    public void saveShop2Redis(Long id,Long expireTime){
        //查询店铺数据
        Shop shop = getById(id);
        //封装逻辑过期时间
        RedisData redisData = new RedisData();
        redisData.setData(shop);
        redisData.setExpireTime(LocalDateTime.now().plusSeconds(expireTime));
        //写入Redis
        stringRedisTemplate.opsForValue().set(CACHE_SHOP_KEY + id,JSONUtil.toJsonStr(redisData));
    }
}
