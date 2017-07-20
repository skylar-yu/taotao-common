package com.taotao.common.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.ShardedJedis;
import redis.clients.jedis.ShardedJedisPool;

import javax.annotation.Resource;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Component("redisCache")
public class RedisCache {
	
	@Resource(name="readShardedJedisPool")
	private ShardedJedisPool readShardedJedisPool;

	@Resource(name="writeJedisPool")
	private JedisPool writeJedisPool;

	/**
	 * 默认 1小时过期
	 */
	private int seconds = 3600;

	/**
	 * 是否启用缓存
	 */
	private boolean isEnable = false;

	private Logger logger = LoggerFactory.getLogger(RedisCache.class);

	/**
	 * 根据 key 从缓存中获取数据
	 * @param key
	 * @return
	 */
	public String get(String key) {
		if (isEnable) {
			ShardedJedis jedis = null;
			long stime = System.currentTimeMillis();
			try {
				jedis = readShardedJedisPool.getResource();
				logger.debug("redis job id={}, get a resource from the pool. key={}, costTime={}ms" ,new Object[]{stime, key, System.currentTimeMillis() - stime});
				return jedis.get(key);
			} catch (Exception e) {
				logger.error("get error key=" + key, e);
			} finally {
				logger.debug("redis job id={}, get the value of the specified key. key={}, costTime={}ms" ,new Object[]{stime, key, System.currentTimeMillis() - stime});
				if(null != jedis){
					readShardedJedisPool.returnResource(jedis);
				}
				logger.debug("redis job id={}, return a resource from the pool. key={}, costTime={}ms" ,new Object[]{stime, key,System.currentTimeMillis() - stime});
			}
		}
		return null;
	}
	
	/**
	 * 根据 key 从缓存中获取数据（指定从master读取）
	 * @param key
	 * @return
	 */
	public String get(String key, boolean fromMaster) {
		if (isEnable) {
			if(fromMaster){
				/*指定从master redis读取记录，排除可能存在master短时间有变动未同步到slave，从集群中读取的并不是最新内容*/
				Jedis jedis = null;
				try {
					jedis = writeJedisPool.getResource();
					return jedis.get(key);
				} catch (Exception e) {
					logger.error("get error key=" + key, e);
				} finally {
					if(null != jedis){
						writeJedisPool.returnResource(jedis);
					}
				}
			}else{
				return get(key);
			}
		}
		return null;
	}
	
	/**
	 * 加入新的key-value键值对（Set if Not eXists，只在key值不存在的情况下才设置）
	 * 如果不指定时间，将使用默认seconds
	 * @param key
	 * @param value
	 * @return
	 */
	public Long setnx(String key, String value) {
		return setnx(key, value, seconds);
	}
	
	/**
	 * 加入新的key-value键值对（Set if Not eXists，只在key值不存在的情况下才设置）
	 * @param key
	 * @param value
	 * @return
	 */
	public Long setnx(String key, String value ,int seconds) {
		if (isEnable) {
			Jedis jedis = null;
			try {
				jedis = writeJedisPool.getResource();
				long result = jedis.setnx(key, value);
				if(result==1){
					/*
					 * 设置新key后才设置过期时间。
					 * 如果key值已存在，仍然采用之前设置的过期时间。
					 */
					jedis.expire(key, seconds);
				}
				logger.debug(key+"-"+result);
				return result;
			} catch (Exception e) {
				logger.error("setnx error key=" + key, e);
			} finally {
				if(null != jedis){
					writeJedisPool.returnResource(jedis);
				}
			}
		}
		return 0L;
	}
	
	/**
	 * 加入新的key-value键值对
	 * 如果不指定时间，将使用默认seconds
	 * @param key
	 * @param value
	 * @return
	 */
	public Boolean set(String key, String value) {
		return set(key, value, seconds);
	}

	/**
	 * 加入新的key-value键值对
	 * @param key
	 * @param value
	 * @return
	 */
	public Boolean set(String key, String value ,int seconds) {
		if (isEnable) {
			Jedis jedis = null;
			long stime = System.currentTimeMillis();
			try {
				jedis = writeJedisPool.getResource();
				logger.debug("1.redis job id={}, get a resource from the pool.costTime={}ms", new Object[]{stime, System.currentTimeMillis() - stime});
				jedis.set(key, value);
				jedis.expire(key, seconds);
				return true;
			} catch (Exception e) {
				logger.error("set error key=" + key, e);
			} finally {
				logger.debug("2.redis job id={}, set the string value as value of the key. key={}, costTime={}ms" ,new Object[]{stime, key, System.currentTimeMillis() - stime});
				if(null != jedis){
					writeJedisPool.returnResource(jedis);
				}
				logger.debug("3.redis job id={}, return a resource from the pool.costTime={}ms" ,new Object[]{stime, System.currentTimeMillis() - stime});
			}
		}
		return false;
	}

	/**
	 * 删除指定key
	 * @param key
	 * @return
	 */
	public Long del(String key) {
		if (isEnable) {
			Jedis jedis = null;
			try {
				logger.debug("remove the specified keys :" + key);
				jedis = writeJedisPool.getResource();
				return jedis.del(key);
			} catch (Exception e) {
				logger.error("del error key=" + key, e);
			} finally {
				if(null != jedis){
					writeJedisPool.returnResource(jedis);
				}
			}
		}
		return null;
	}

	/**
	 * 向list头部加入值
	 * @param key
	 * @param strings
	 * @return
	 */
	public Boolean lpush(String key, String... strings) {
		if (isEnable) {
			Jedis jedis = null;
			try {
				logger.info("add the string value to the head (LPUSH) of the list stored at key. key=" + key);
				jedis = writeJedisPool.getResource();
				for(String string:strings){
					jedis.lpush(key, string);
				}
				jedis.expire(key, seconds);
				return true;
			} catch (Exception e) {
				logger.error("lpush error key=" + key, e);
			} finally {
				if(null != jedis){
					writeJedisPool.returnResource(jedis);
				}
			}
		}
		return false;
	}
	
	/**
	 * 向list尾部加入值
	 * @param key
	 * @param strings
	 * @return
	 */
	public Boolean rpush(String key, String... strings) {
		if (isEnable) {
			Jedis jedis = null;
			try {
				logger.info("add the string value to the tail (RPUSH) of the list stored at key. key=" + key);
				jedis = writeJedisPool.getResource();
				for(String string:strings){
					jedis.rpush(key, string);
				}
				jedis.expire(key, seconds);
				return true;
			} catch (Exception e) {
				logger.error("rpush error key=" + key, e);
			} finally {
				if(null != jedis){
					writeJedisPool.returnResource(jedis);
				}
			}
		}
		return false;
	}
	
	/**
	 * 返回list中指定范围的元素
	 * @param key
	 * @param start
	 * @param end
	 * @return
	 */
	public List<String> lrange(String key, long start, long end) {
		if (isEnable) {
			ShardedJedis jedis = null;
			try {
				logger.info("return the specified elements of the list stored at the specified key. key=" + key);
				jedis = readShardedJedisPool.getResource();
				return jedis.lrange(key, start, end);
			} catch (Exception e) {
				logger.error("lrange error key=" + key, e);
			} finally {
				if(null != jedis){
					readShardedJedisPool.returnResource(jedis);
				}
			}
		}
		return null;
	}
	
	/**
	 * 返回list长度
	 * @param key
	 * @return
	 */
	public Long llen(String key) {
		if (isEnable) {
			ShardedJedis jedis = null;
			try {
				logger.info("return the length of the list stored at the specified key. key=" + key);
				jedis = readShardedJedisPool.getResource();
				return jedis.llen(key);
			} catch (Exception e) {
				logger.error("llen error key=" + key, e);
			} finally {
				if(null != jedis){
					readShardedJedisPool.returnResource(jedis);
				}
			}
		}
		return null;
	}

	/**
	 * 向指定key的set集合中加入新成员
	 * @param key
	 * @param members
	 * @param seconds
	 * @return
	 */
	public Boolean sadd(String key, String... members) {
		if (isEnable) {
			Jedis jedis = null;
			try {
				logger.info("add the specified member to the set value stored at key. key=" + key);
				jedis = writeJedisPool.getResource();
				for(String member:members){
					jedis.sadd(key, member);
				}
				jedis.expire(key, seconds);
				return true;
			} catch (Exception e) {
				logger.error("sadd error key=" + key, e);
			} finally {
				if(null != jedis){
					writeJedisPool.returnResource(jedis);
				}
			}
		}
		return false;
	}

	/**
	 * 判断某个成员是否属于指定key的set集合
	 * 
	 * @param
	 * @return boolean
	 */
	public Boolean sismember(String key, String member) {
		if (isEnable) {
			ShardedJedis jedis = null;
			try {
				logger.info("if member is a member of the set stored at key. key=" + key);
				jedis = readShardedJedisPool.getResource();
				return jedis.sismember(key, member);
			} catch (Exception e) {
				logger.error("sismenber error key=" + key, e);
			} finally {
				if(null != jedis){
					readShardedJedisPool.returnResource(jedis);
				}
			}
		}
		return null;
	}
	
	/**
	 * 返回指定key的set集合
	 * @param
	 * @return boolean
	 */
	public Set<String> smembers(String key) {
		if (isEnable) {
			ShardedJedis jedis = null;
			try {
				logger.info("return all the members (elements) of the set value stored at key. key=" + key);
				jedis = readShardedJedisPool.getResource();
				return jedis.smembers(key);
			} catch (Exception e) {
				logger.error("smembers error key=" + key, e);
			} finally {
				if(null != jedis){
					readShardedJedisPool.returnResource(jedis);
				}
			}
		}
		return null;
	}

	/**
	 * 删除存储在key中指定member的集合成员
	 * @param key
	 * @param members
	 * @return
	 */
	public Long srem(String key, String... members) {
		if (isEnable) {
			Jedis jedis = null;
			try {
				logger.info("remove the specified member from the set value stored at key. key=" + key);
				jedis = writeJedisPool.getResource();
				for(String member:members){
					jedis.srem(key, member);
				}
			} catch (Exception e) {
				logger.error("srem error key=" + key, e);
			} finally {
				if(null != jedis){
					writeJedisPool.returnResource(jedis);
				}
			}
		}
		return null;
	}

	/**
	 * 返回指定key中set的元素个数
	 * @param key
	 * @return
	 */
	public Long scard(String key) {
		if (isEnable) {
			ShardedJedis jedis = null;
			try {
				logger.info("return the set cardinality (number of elements). key="+ key);
				jedis = readShardedJedisPool.getResource();
				return jedis.scard(key);
			} catch (Exception e) {
				logger.error("scard error key=" + key, e);
			} finally {
				if(null != jedis){
					readShardedJedisPool.returnResource(jedis);
				}
			}
		}
		return null;
	}


	/**
	 * 为指定的key设定Filed/Value对， 如果Key不存在，该命令将创建新Key设置Field/Value对
	 * 如果参数中的Field在该Key中已经存在，则用新值覆盖其原有值
	 * @param key
	 * @param field
	 * @param value
	 * @return
	 */
	public boolean hset(String key, String field, String value) {
		if (isEnable) {
			Jedis jedis = null;
			try {
				logger.info("set the specified hash field to the specified value. key="+ key);
				jedis = writeJedisPool.getResource();
				jedis.hset(key, field, value);
				jedis.expire(key, seconds);
				return true;
			} catch (Exception e) {
				logger.error("hset error key=" + key, e);
			} finally {
				if(null != jedis){
					writeJedisPool.returnResource(jedis);
				}
			}
		}
		return false;
	}

	/**
	 * 从指定key的Hash列表中返回参数Field的关联值
	 * @param key
	 * @param field
	 * @return
	 */
	public String hget(String key, String field) {
		if (isEnable) {
			ShardedJedis jedis = null;
			try {
				logger.info("retrieve the value associated to the specified field. key="+ key);
				jedis = readShardedJedisPool.getResource();
				return jedis.hget(key, field);
			} catch (Exception e) {
				logger.error("hget error key=" + key, e);
			} finally {
				if(null != jedis){
					readShardedJedisPool.returnResource(jedis);
				}
			}
		}
		return null;
	}

	/**
	 * 从指定Key的Hash队列中删除Field的关联值
	 * @param key
	 * @param field
	 * @return
	 */
	public Long hdel(String key, String field) {
		if (isEnable) {
			Jedis jedis = null;
			try {
				logger.info("remove the specified field from an hash stored at key. key="+ key);
				jedis = writeJedisPool.getResource();
				return jedis.hdel(key, field);
			} catch (Exception e) {
				logger.error("hdel error key=" + key, e);
			} finally {
				if(null != jedis){
					writeJedisPool.returnResource(jedis);
				}
			}
		}
		return null;
	}

	/**
	 * 返回该Key对应的hash表中的实体数量 如果Key值不存在，返回0
	 * 
	 * @param key
	 * @return
	 */
	public Long hlen(String key) {
		if (isEnable) {
			ShardedJedis jedis = null;
			try {
				logger.info("the number of items in a hash. key" + key);
				jedis = readShardedJedisPool.getResource();
				return jedis.hlen(key);
			} catch (Exception e) {
				logger.error("hlen error key=" + key, e);
			} finally {
				if(null != jedis){
					readShardedJedisPool.returnResource(jedis);
				}
			}
		}
		return (long) 0;
	}

	/**
	 * 返回Key对应的hash表中所有键值对
	 * @param key
	 * @return
	 */
	public Map<String, String> hgetAll(String key) {
		if (isEnable) {
			ShardedJedis jedis = null;
			try {
				logger.info("all the fields and associated values in a hash. key="+ key);
				jedis = readShardedJedisPool.getResource();
				return jedis.hgetAll(key);
			} catch (Exception e) {
				logger.error("hgetAll error key=" + key, e);
			} finally {
				if(null != jedis){
					readShardedJedisPool.returnResource(jedis);
				}
			}
		}
		return null;
	}

	/**
	 * 从指定Key的hash中获取多个fields对应的value值 
	 * @param key
	 * @param fields
	 * @return
	 */
	public List<String> hmget(String key, String... fields) {
		if (isEnable) {
			ShardedJedis jedis = null;
			try {
				logger.info("retrieve the values associated to the specified fields. key=" + key);
				jedis = readShardedJedisPool.getResource();
				return jedis.hmget(key, fields);
			} catch (Exception e) {
				logger.error("hmget error key=" + key, e);
			} finally {
				if(null != jedis){
					readShardedJedisPool.returnResource(jedis);
				}
			}
		}
		return null;
	}

	/**
	 * 向指定Key的hash中设置多个fields-value值
	 * @param key
	 * @param hash
	 * @return
	 */
	public String hmset(String key, Map<String, String> hash) {
		if (isEnable) {
			Jedis jedis = null;
			try {
				logger.info("retrieve the values associated to the specified fields. key=" + key);
				jedis = writeJedisPool.getResource();
				return jedis.hmset(key, hash);
			} catch (Exception e) {
				logger.error("hmget error key=" + key, e);
			} finally {
				if(null != jedis){
					writeJedisPool.returnResource(jedis);
				}
			}
		}
		return null;
	}

	/**
	 * 重置缓存过期时间（自定义时间）
	 * @param key
	 * @return
	 */
	public Long expire(String key, int seconds) {
		if (isEnable) {
			Jedis jedis = null;
			try {
				logger.info("set a timeout on the specified key. key=" + key);
				jedis = writeJedisPool.getResource();
				return jedis.expire(key, seconds);
			} catch (Exception e) {
				logger.error("expire error key=" + key, e);
			} finally {
				if(null != jedis){
					writeJedisPool.returnResource(jedis);
				}
			}
		}
		return null;
	}
	
	/**
	 * 重置缓存过期时间（默认时间）
	 * @param key
	 * @return
	 */
	public Long expire(String key) {
		return expire(key, seconds);
	}
	
	public int getSeconds() {
		return seconds;
	}

	public void setSeconds(int seconds) {
		this.seconds = seconds;
	}

	public boolean isEnable() {
		return isEnable;
	}

	public void setIsEnable(boolean isEnable) {
		this.isEnable = isEnable;
	}
}
