package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.bean.copier.CopyOptions;
import cn.hutool.core.lang.UUID;
import cn.hutool.core.util.RandomUtil;
import com.baomidou.mybatisplus.extension.conditions.query.QueryChainWrapper;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.dto.LoginFormDTO;
import com.hmdp.dto.Result;
import com.hmdp.dto.UserDTO;
import com.hmdp.entity.User;
import com.hmdp.mapper.UserMapper;
import com.hmdp.service.IUserService;
import com.hmdp.utils.RegexUtils;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import javax.servlet.http.HttpSession;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.hmdp.utils.RedisConstants.*;
import static com.hmdp.utils.SystemConstants.USER_NICK_NAME_PREFIX;

/**
 * <p>
 * 服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Slf4j
@Service
public class UserServiceImpl extends ServiceImpl<UserMapper, User> implements IUserService {

    @Autowired
    private StringRedisTemplate stringRedisTemplate;

    @Override
    public Result sendCode(String phone, HttpSession session) {
        //1. 验证手机号是否有效
        if (RegexUtils.isPhoneInvalid(phone)) {
            return Result.fail("无效的手机号");
        }
        //2. 生成验证码
        String code = RandomUtil.randomNumbers(6);
        //3. 保存验证码到Redis并设置有效期
        //session.setAttribute("code",code);
        stringRedisTemplate.opsForValue().set(LOGIN_CODE_KEY + phone,code,LOGIN_CODE_TTL, TimeUnit.MINUTES);
        //4. 发送验证码
        log.debug("短信验证码发送成功：{}",code);
        return Result.ok();
    }

    @Override
    public Result login(LoginFormDTO loginForm, HttpSession session) {
        //1. 验证手机号
        String phone = loginForm.getPhone();
        if (RegexUtils.isPhoneInvalid(phone)) {
            return Result.fail("无效的手机号");
        }
        //2. 验证验证码(根据手机号从Redis中取验证码)
        //Object cacheCode = session.getAttribute("code");
        String cacheCode = stringRedisTemplate.opsForValue().get(LOGIN_CODE_KEY + phone);
        String code = loginForm.getCode();
        if(cacheCode == null || !cacheCode.equals(code)){
            return Result.fail("验证码错误");
        }
        //3. 根据手机号查询用户 select * from user where phone = ?
        User user = query().eq("phone", phone).one();
        //4. 用户不存在，创建用户
        if(user == null){
            user = createUser(phone);
        }
        //5. 生成Token
        String token = UUID.randomUUID(true).toString();
        //6. 将用户信息user转为HashMap结构
        UserDTO userDTO = BeanUtil.copyProperties(user, UserDTO.class);
        Map<String, Object> userMap = BeanUtil.beanToMap(userDTO, new HashMap<>(), CopyOptions.create()
                .setIgnoreNullValue(true)
                .setFieldValueEditor((fieldName, fieldValue) -> fieldValue.toString()));
        //7. 保存用户到Redis并设置过期时间（使用hash结构）
        //session.setAttribute("user", BeanUtil.copyProperties(user, UserDTO.class));
        stringRedisTemplate.opsForHash().putAll(LOGIN_USER_KEY + token,userMap);
        stringRedisTemplate.expire(LOGIN_USER_KEY + token,LOGIN_USER_TTL,TimeUnit.MINUTES);
        return Result.ok(token);
    }

    private User createUser(String phone) {
        User user = new User();
        user.setPhone(phone);
        user.setNickName(USER_NICK_NAME_PREFIX + RandomUtil.randomString(5));
        save(user);
        return user;
    }
}
