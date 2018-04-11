package org.seckill.dao.cache;

import org.seckill.entity.Seckill;
import org.seckill.utils.SerializableHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

/**
 * Created by Administrator on 2016/12/20.
 */
public class RedisDao {
	
	private final Logger logger = LoggerFactory.getLogger(RedisDao.class);
	
	private final JedisPool jedisPool;

	public RedisDao(String ip, int port) {
		jedisPool = new JedisPool(ip, port);
	}
	
	private SerializableHelper<Seckill> serizableHelper=new SerializableHelper<Seckill>(Seckill.class);
	
	
	public Seckill getSeckill(String seckillId) {
		try {
			Jedis jedis = jedisPool.getResource();
			try {
				String key = seckillId;
				byte[] bytes = jedis.get(key.getBytes());
				if (bytes != null) {
					Seckill seckill = serizableHelper.deserialize(bytes);
					return seckill;
				}
			} finally {
				jedis.close();
			}
		} catch (Exception e) {
			logger.error("",e);
		}
		return null;
	}

	/**
	 * 
	 * @param seckill
	 * @return
	 */
	public String putSeckill(Seckill seckill) {
		try {
			Jedis jedis = jedisPool.getResource();
			try {
				String key = "seckill:" + seckill.getSeckillId();
				byte[] bytes =serizableHelper.serialize(seckill);
				int timeout = 60 * 60;
				String result = jedis.setex(key.getBytes(), timeout, bytes);
				return result;
			} finally {
				jedis.close();
			}
		} catch (Exception e) {
			logger.error("",e);
		}
		return null;
	}
}