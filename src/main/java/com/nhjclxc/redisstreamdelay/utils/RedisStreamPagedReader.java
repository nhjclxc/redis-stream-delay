package com.nhjclxc.redisstreamdelay.utils;

import org.springframework.data.domain.Range;
import org.springframework.data.redis.connection.RedisZSetCommands;
import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.data.redis.core.StreamOperations;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;

/**
 * 基于 Redis Stream 的分页读取工具方法，它支持从 Redis Stream 中分批读取消息，避免一次性加载过多数据导致内存压力。
 */
@Component
public class RedisStreamPagedReader {

    private final StringRedisTemplate redisTemplate;

    public RedisStreamPagedReader(StringRedisTemplate redisTemplate) {
        this.redisTemplate = redisTemplate;
    }

    /**
     * 分页读取 Redis Stream 的所有记录
     *
     * @param streamKey Stream 的 key
     * @param pageSize  每页读取条数
     * @return 所有记录组成的列表
     */
    public List<MapRecord<String, Object, Object>> readAll(String streamKey, int pageSize) {
        List<MapRecord<String, Object, Object>> result = new ArrayList<>();
        StreamOperations<String, Object, Object> ops = redisTemplate.opsForStream();

        String lastId = "0-0";
        boolean hasMore = true;

        while (hasMore) {
            // 设置分页范围，从上次 ID（不含）开始
            Range<String> range = Range.open(lastId, "+");
            RedisZSetCommands.Limit limit = RedisZSetCommands.Limit.limit();
            limit.count(pageSize);

            List<MapRecord<String, Object, Object>> page = ops.range(streamKey, range, limit);
            if (page == null || page.isEmpty()) {
                hasMore = false;
            } else {
                result.addAll(page);
                // 更新 lastId 为最后一条消息的 ID
                lastId = page.get(page.size() - 1).getId().getValue();
            }
        }

        return result;
    }


    /**
     * 只读当前时间前的消息
     *
     * 利用消息 ID 进行按时间过滤（强烈推荐）
     * Redis Stream 的 ID 通常为 时间戳-seq（如 1715234000000-0），你可以设置一个上限时间，只读取“当前时间之前的部分”，避免全量扫描。
     *
     * @param streamKey
     * @param pageSize
     * @param expireBeforeMillis
     * @return
     */
    public List<MapRecord<String, Object, Object>> readExpired(String streamKey, int pageSize, long expireBeforeMillis) {
        List<MapRecord<String, Object, Object>> result = new ArrayList<>();
        StreamOperations<String, Object, Object> ops = redisTemplate.opsForStream();

        String lastId = "0-0";
        boolean hasMore = true;

        String maxId = expireBeforeMillis + "-0";

        while (hasMore) {
            Range<String> range = Range.open(lastId, maxId); // 只读 expireBeforeMillis 之前的
            RedisZSetCommands.Limit limit = RedisZSetCommands.Limit.limit();
            limit.count(pageSize);

            List<MapRecord<String, Object, Object>> page = ops.range(streamKey, range, limit);
            if (page == null || page.isEmpty()) {
                hasMore = false;
            } else {
                result.addAll(page);
                lastId = page.get(page.size() - 1).getId().getValue();
            }
        }

        return result;
    }

}
