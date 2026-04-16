package mes.app.util;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

@Service
@Slf4j
public class RedisService {

    private final RedisTemplate<String, Object> redisTemplate;

    //localCache 조회
    @Getter
    private final Map<String, Object> localCache = new ConcurrentHashMap<>();

    public RedisService(RedisTemplate<String, Object> redisTemplate) {
        this.redisTemplate = redisTemplate;
    }

    // 저장
    public void setValues(String key, Object value){
        try{
            redisTemplate.opsForValue().set(key, value);
        }catch(Exception e){
            log.error("Redis 연결실패- 로컬 메모리 저장");
            localCache.put(key, value);
        }
    }

    // 저장, 만료시간 설정
    public void setValues(String key, Object value, long duration, TimeUnit unit){
        try{
            redisTemplate.opsForValue().set(key,value, duration,unit);
        }catch (Exception e){
            localCache.put(key, value);
        }
    }

    // 3. 데이터 조회 (GET)
    public Object getValues(String key) {
        try{
            return redisTemplate.opsForValue().get(key);
        }catch (Exception e){
            log.warn("Redis 조회실패 - 로컬 메모리에서 조회");
            return localCache.get(key);
        }
    }

    // 4. [핵심] API 카운트용 숫자 증가 (INCR)
    public Long incrementValue(String key) {

        try{
            Long count = redisTemplate.opsForValue().increment(key);

            if(count != null && count == 1){
                redisTemplate.expire(key, 35, TimeUnit.DAYS);
            }
            return count;
        }catch (Exception e){
            log.error("Redis INCR 실패 - 로컬 메모리 카운팅");
            return (Long) localCache.compute(key, (k, v) -> (v == null) ? 1L : (long) v + 1L);
        }
    }

    // hash 구조로 set
    public Long incrementHashValue(String key, String field){
        try{

            Long count = redisTemplate.opsForHash().increment(key, field, 1L);

            // Hash 전체 키에 대한 TTL입니다.
            if (count != null && count == 1) {
                redisTemplate.expire(key, 45, TimeUnit.DAYS);
            }
            return count;
        }catch(Exception e){
            log.error("Redis HINCRBY 실패 - Key: {}, Field: {}", key, field);
            // 로컬 캐시 처리 시 키와 필드를 조합해서 저장
            String localKey = key + ":" + field;
            return (Long) localCache.compute(localKey, (k, v) -> (v == null) ? 1L : (long) v + 1L);
        }
    }

    public Long incrementValue(String key, Long value){
        try{
            Long count = redisTemplate.opsForValue().increment(key, value);
            return  count;
        }catch(Exception e){
            log.error("Redis 연결 실패! 로컬 캐시에 임시 저장합니다. Key: {}, Value: {}", key, value);

            // 로컬 캐시에 합산 (기존 값이 없으면 value, 있으면 합산)
            // compute 메서드는 ConcurrentHashMap의 원자적 연산을 보장합니다.
            localCache.merge(key, value, (oldVal, newVal) -> (Long) oldVal + (Long) newVal);

            return -1L; // 에러 발생 신호로 -1 반환 (또는 적절한 값)
        }
    }

    // 5. 데이터 삭제 (DEL)
    public void deleteValues(String key) {
        try{
            redisTemplate.delete(key);
        }catch (Exception e){
            localCache.remove(key);
        }
    }


    //패턴으로 조회
    public Map<String, Integer> getValuesByPattern(String pattern){
        try{
            Set<String> keys = redisTemplate.keys(pattern);
            Map<String, Integer> usageMap = new HashMap<>();

            if (keys != null) {
                for (String key : keys) {
                    // 2. 각 키에 해당하는 값 조회
                    Object value = redisTemplate.opsForValue().get(key);

                    // 3. 값이 String이면 파싱, 이미 Integer 형태라면 바로 저장 (안전한 형변환)
                    if (value != null) {
                        int count = Integer.parseInt(value.toString());
                        usageMap.put(key, count);
                    }
                }
            }
            return usageMap;
        }catch (Exception e){
            log.warn("Redis 패턴 조회 실패 - 로컬 메모리에서 패턴 조회 실행");
            return localCache.entrySet().stream()
                    .filter(entry -> entry.getKey().contains(pattern.replace("*", "")))
                    .collect(Collectors.toMap(
                            Map.Entry::getKey,
                            entry -> Integer.parseInt(entry.getValue().toString())
                    ));
        }
    }

    //현재 레디스가 연결된 상태인지 확인
    public boolean isRedisAvailable(){
        try {
            String pong = redisTemplate.getConnectionFactory().getConnection().ping();
            return "PONG".equals(pong);
        }catch(Exception e){
            log.error("Redis 연결상태 확인 실패");
            return false;
        }
    }

}
