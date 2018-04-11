package org.seckill.service.impl;

import org.apache.commons.collections.MapUtils;
import org.seckill.dao.SeckillDao;
import org.seckill.dao.SuccessKilledDao;
import org.seckill.dao.cache.RedisDao;
import org.seckill.dto.Exposer;
import org.seckill.dto.SeckillExecution;
import org.seckill.entity.Seckill;
import org.seckill.entity.SuccessKilled;
import org.seckill.enums.SeckillStatEnum;
import org.seckill.exception.RepeatKillException;
import org.seckill.exception.SeckillCloseException;
import org.seckill.exception.SeckillException;
import org.seckill.service.SeckillService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.DigestUtils;
import org.springframework.util.StringUtils;

import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by moonie on 16/5/30.
 */
@Service
public class SeckillServiceImpl implements SeckillService{

    private Logger logger = LoggerFactory.getLogger(this.getClass());

    @Autowired
    private SeckillDao seckillDao;

    @Autowired
    private SuccessKilledDao successKilledDao;
    
    @Autowired
    private RedisDao redisDao;

    private final String slat = "fsadjfweijp9up98239(&(@(&*@";

    public List<Seckill> getSeckillList() {
        return seckillDao.queryAll(0,10);
    }

    public Seckill getById(long seckillId) {
        return seckillDao.queryById(seckillId);
    }
    /*
     * 获取秒杀url ---redis的缓存优化
     */
    public Exposer exportSeckillUrl(long seckillId) {
    	//缓存优化
        /*Seckill seckill = seckillDao.queryById(seckillId);
        if(seckill == null){
            return new Exposer(false,seckillId);
        }
        Date startTime = seckill.getStartTime();
        Date endTime = seckill.getEndTime();

        Date nowTime = new Date();
        if(nowTime.getTime() < startTime.getTime()
                || nowTime.getTime() > endTime.getTime()){
            return new Exposer(false,seckillId,nowTime.getTime(),startTime.getTime(),endTime.getTime());
        }
        String md5 = getMD5(seckillId);
        return new Exposer(true,md5,seckillId);*/
    	String key = "seckill:" + seckillId;
    	Seckill seckill = redisDao.getSeckill(key);
    	if (null == seckill) {
			//从数据库取值
    		seckill = seckillDao.queryById(seckillId);
    		if (null == seckill) {
    			return new Exposer(false,seckillId);
			}else {
				redisDao.putSeckill(seckill);
			}
		}
    	Date startTime = seckill.getStartTime();
        Date endTime = seckill.getEndTime();

        Date nowTime = new Date();
        if(nowTime.getTime() < startTime.getTime()
                || nowTime.getTime() > endTime.getTime()){
            return new Exposer(false,seckillId,nowTime.getTime(),startTime.getTime(),endTime.getTime());
        }
        String md5 = getMD5(seckillId);
        return new Exposer(true,md5,seckillId);
    }

    private String getMD5(long seckillId){
        String base = seckillId + "/" + slat;
        String md5 = DigestUtils.md5DigestAsHex(base.getBytes());
        return md5;
    }

    /**
     * 使用注解配置的优点:
     * 1.开发团队达成一致,明确标注事务方法的编程风格
     * 2.保证事务方法的时间尽可能短,不要穿插其他网络操作(RPC/HTTP请求),需要的话应该放到事务方法外部
     * 3.不是所有的方法都需要事务,如只有一条修改操作、只读操作不需要
     */
    @Transactional
    public SeckillExecution executeSeckill(long seckillId, long userPhone, String md5)
            throws SeckillCloseException, RepeatKillException, SeckillException {
        if(md5 == null || !md5.equals(getMD5(seckillId))){
            throw new SeckillException("seckill data rewrite");
        }
        Date now = new Date();
        try{
            int updateCount = seckillDao.reduceNumber(seckillId,now);
            if(updateCount <= 0) {
                throw new SeckillCloseException("seckill is closed");
            }else{
                int insertCount = successKilledDao.insertSuccessKilled(seckillId,userPhone);
                if(insertCount <= 0){
                    throw new RepeatKillException("seckill repeat");
                }else{
                    SuccessKilled successKilled = successKilledDao.queryByIdWithSeckill(seckillId,userPhone);
                    return new SeckillExecution(seckillId, SeckillStatEnum.SUCCESS,successKilled);
                }
            }
        }catch (SeckillCloseException e1){
            throw e1;
        }catch (RepeatKillException e2){
            throw e2;
        }catch (Exception e){
            logger.error(e.getMessage(),e);
            throw new SeckillException("seckill inner error:"+e.getMessage());
        }
    }
    /**
     * 通过存储过程实现秒杀
     */
	@Override
	public SeckillExecution executeSeckillProcedure(long seckillId,
			long userPhone, String md5) {
		  if (StringUtils.isEmpty(md5) || !md5.equals(getMD5(seckillId))) {
			  throw new SeckillException(
						SeckillStatEnum.DATA_REWRITE.getStateInfo());
		}
		  Date killTime = new Date();
		  Map<String,Object> map = new HashMap<String,Object>();
		  map.put("seckillId", seckillId);
		  map.put("phone", userPhone);
		  map.put("killTime", killTime);
		  map.put("result", null);
			// 执行存储过程 result被赋值
		  try {
			
			  seckillDao.killByProcedure(map);
			  Integer result = MapUtils.getInteger(map, "result", -2);
			  if (result == 1) {
				SuccessKilled sk = successKilledDao.queryByIdWithSeckill(seckillId, userPhone);
				return new SeckillExecution(seckillId, SeckillStatEnum.SUCCESS,sk);
			}else {
				return new SeckillExecution(seckillId, SeckillStatEnum.stateOf(result));
			}
		} catch (Exception e) {
			logger.error(e.getMessage());
			return new SeckillExecution(seckillId, SeckillStatEnum.INNER_ERROR);
		}
	}
}