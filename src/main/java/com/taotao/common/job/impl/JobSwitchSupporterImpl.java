package com.taotao.common.job.impl;

import com.google.common.base.Function;
//import com.oneplus.wms.common.JobSwitchSupporter;
//import com.oneplus.wms.common.utils.RedisCache;
import com.taotao.common.job.JobSwitchSupporter;
import com.taotao.common.util.RedisCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * 功能描述：支持定时任务开关门阀接口实现
 *
 * @author: Zhenbin.Li
 * email： lizhenbin@oneplus.cn
 * company：一加科技
 * Date: 16/7/14 Time: 17:58
 */
public class JobSwitchSupporterImpl implements JobSwitchSupporter {

    /**
     * sl4j
     */
    private static final Logger LOGGER = LoggerFactory.getLogger(JobSwitchSupporterImpl.class);

    @Autowired
    private RedisCache redisCache;

    @Override
    public <F, T> void supporter(JobRedisCacheKey jobRedisCacheKey, Function<F, T> execute) {
        supporter(jobRedisCacheKey, execute, null);
    }

    @Override
    public <F, T> T supporter(JobRedisCacheKey jobRedisCacheKey, Function<F, T> execute, F input) {
        boolean addJobRedisCacheKey = true;
        try {
            addJobRedisCacheKey = addJobRedisCacheKey(jobRedisCacheKey);
            if (!addJobRedisCacheKey) {
                return null;
            }

            return execute.apply(input);

        } catch (Exception ex) {
            // 产生异常,则直接标志删除KEY
            addJobRedisCacheKey = true;
            LOGGER.error("执行定时任务异常, jobRedisCacheKey={}", jobRedisCacheKey, ex);
        } finally {
            if (addJobRedisCacheKey) {
                removeJobRedisCacheKey(jobRedisCacheKey);
            }
        }

        return null;
    }

    /**
     * 设置redis key对应的内容
     *
     * @param jobRedisCacheKey
     */
    protected boolean addJobRedisCacheKey(JobRedisCacheKey jobRedisCacheKey) {
        if (this.redisCache != null && jobRedisCacheKey != null) {
            Long setnx = this.redisCache.setnx(jobRedisCacheKey.getKey(), jobRedisCacheKey.getValue(), jobRedisCacheKey.getDelayTime());
            if (setnx == null || setnx <= 0) {
                LOGGER.info("定时任务门阀JobRedisCacheKey尚未打开, 跳过当次任务, jobRedisCacheKey={}", jobRedisCacheKey);
                return false;
            }
            LOGGER.info("设置定时任务门阀开关成功, jobRedisCacheKey={}", jobRedisCacheKey);
            return true;
        }

        return false;
    }

    /**
     * 删除redis key对应的内容
     *
     * @param jobRedisCacheKey
     */
    protected void removeJobRedisCacheKey(JobRedisCacheKey jobRedisCacheKey) {
        if (this.redisCache != null && jobRedisCacheKey != null) {
            this.redisCache.del(jobRedisCacheKey.getKey());
            LOGGER.info("删除定时任务门阀开关成功, jobRedisCacheKey={}", jobRedisCacheKey);
        } else {
            throw new RuntimeException("jobRedisCacheKey为空!");
        }
    }
}
