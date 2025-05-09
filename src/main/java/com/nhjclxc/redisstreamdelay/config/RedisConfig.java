package com.nhjclxc.redisstreamdelay.config;

import com.nhjclxc.redisstreamdelay.service.DelayTaskService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.Message;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.listener.PatternTopic;
import org.springframework.data.redis.listener.RedisMessageListenerContainer;
import org.springframework.data.redis.listener.adapter.MessageListenerAdapter;

import javax.annotation.Resource;
import java.nio.charset.StandardCharsets;
import java.time.Duration;

/**
 * redis配置类
 */
@Configuration
public class RedisConfig {

    @Autowired
    private DelayTaskService delayTaskService;

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Bean
    public StringRedisTemplate stringRedisTemplate(RedisConnectionFactory connectionFactory) {
        return new StringRedisTemplate(connectionFactory);
    }

    /**
     * 注册key过期监听器
     */
    @Bean
    public RedisMessageListenerContainer container(RedisConnectionFactory connectionFactory) {
        RedisMessageListenerContainer container = new RedisMessageListenerContainer();
        container.setConnectionFactory(connectionFactory);
        container.addMessageListener(new MessageListenerAdapter(new ExpiredKeyListener()), new PatternTopic("__keyevent@0__:expired"));
        return container;
    }

    /**
     * key过期监听器处理器函数
     */
    public class ExpiredKeyListener implements org.springframework.data.redis.connection.MessageListener {
        @Override
        public void onMessage(Message message, byte[] pattern) {
            String expiredKey = new String(message.getBody(), StandardCharsets.UTF_8);
            // 判断这个过期的key是否为要监听的key，这里指定了某个前缀为我们要监听处理过期key前缀
            if (expiredKey.startsWith(DelayTaskService.STREAM_DELAY_KEY_PREFIX)) {
                // 去除key前缀，取出真正的业务key数据，用于处理
                String taskId = expiredKey.substring(DelayTaskService.STREAM_DELAY_KEY_PREFIX.length());

                // 分布式锁 防止重复消费
                Boolean locked = stringRedisTemplate.opsForValue().setIfAbsent("lock:task:" + taskId, "1", Duration.ofSeconds(30));
                if (Boolean.TRUE.equals(locked)) {
                    try {
                        // 执行过期处理
                        delayTaskService.processTask(taskId);
                    } finally {
                        stringRedisTemplate.delete("lock:task:" + taskId); // 解锁
                    }
                }

            }
        }
    }
}
