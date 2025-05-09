package com.nhjclxc.redisstreamdelay.service;

import lombok.extern.slf4j.Slf4j;
import org.springframework.data.domain.Range;
import com.nhjclxc.redisstreamdelay.model.DelayTaskDTO;
import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.data.redis.core.StreamOperations;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


@Slf4j
@Service
public class DelayTaskService {

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    private static final String STREAM_KEY = "stream:delay:tasks";
    public static final String STREAM_DELAY_KEY_PREFIX = "stream:delay:trigger:";

    /**
     * 模拟监听任务的添加
     */
    public void addTask(DelayTaskDTO task) {
        long timeMillis = System.currentTimeMillis();
        Map<String, Object> fields = new HashMap<>();
        fields.put("taskId", task.getTaskId());
        fields.put("type", task.getType());
        String content = task.getContent() + " : " + (timeMillis + task.getDelayMillis());
        fields.put("content", content);

        stringRedisTemplate.opsForStream().add(STREAM_KEY, fields);
        stringRedisTemplate.opsForValue().set(DelayTaskService.STREAM_DELAY_KEY_PREFIX + task.getTaskId(), "1", Duration.ofMillis(task.getDelayMillis()));

        log.info("addTask timestamp: {}, task: {}", System.currentTimeMillis(), content);
    }

    /**
     * 处理监听到的key过期事件
     *
     * @param taskId 业务key
     */
    public void processTask(String taskId) {
        if (null == taskId || "".equals(taskId)) {
            return;
        }

        // 从 Redis 中读取一个指定的 Stream（STREAM_KEY）里的所有消息，不加任何起止 ID 限制。
        StreamOperations<String, Object, Object> ops = stringRedisTemplate.opsForStream();
        // 这行是调用 range 方法读取 Redis Stream 中的消息，Range.unbounded() 表示从头到尾读取所有消息（等价于 range(STREAM_KEY, Range.closed("0-0", "+"))）
        List<MapRecord<String, Object, Object>> messageList = ops.range(STREAM_KEY, Range.unbounded());
        if (null == messageList || messageList.size() == 0) {
            return;
        }

        // todo
        // 上面的写法是将 STREAM_KEY 指定的所有过期消息全部读取，下面在用if判断处理，
        // 这样就导致了大量key的时候的服务器压力，如何优化呢？？？
        // 可以看看 RedisStreamPagedReader 的处理方法

        for (MapRecord<String, Object, Object> recordMap : messageList) {
            Map<Object, Object> value = recordMap.getValue();
            if (taskId.equals(String.valueOf(value.get("taskId")))) {
                try {
                    // 打印日志，模拟实际业务处理
                    log.info("Processing timestamp: {}, task: {}", System.currentTimeMillis(), value);

                    // 延迟任务执行完毕，后在redis的stream中生成那条消息记录，避免重复消费
                    stringRedisTemplate.opsForStream().delete(STREAM_KEY, recordMap.getId());
                } catch (Exception e) {
                    log.error("处理任务失败: {}", value, e);
                }

                // 匹配到了，提前退出
                break;
            }
        }
    }
}
