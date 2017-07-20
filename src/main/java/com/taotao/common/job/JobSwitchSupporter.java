package com.taotao.common.job;

import com.google.common.base.Function;
import com.taotao.common.job.impl.JobRedisCacheKey;
//import com.oneplus.wms.common.job.JobRedisCacheKey;

/**
 * 功能描述：支持定时任务开关门阀接口,主要是解决分布式情况下,定时任务抢占资源问题
 *
 * @author: Zhenbin.Li
 * email： lizhenbin@oneplus.cn
 * company：一加科技
 * Date: 16/7/14 Time: 18:18
 */
public interface JobSwitchSupporter {

    /**
     * 定时任务开发门阀选择器
     *
     * @param jobRedisCacheKey cache key
     * @param execute          执行任务内容Function
     * @param <F>              Function 入参类型
     * @param <T>              Function 回调返回类型
     */
    public <F, T> void supporter(JobRedisCacheKey jobRedisCacheKey, Function<F, T> execute);

    /**
     * 定时任务开发门阀选择器,可以回调结果
     *
     * @param jobRedisCacheKey cache key
     * @param execute          执行任务内容Function
     * @param input            Function 入参
     * @param <F>              Function 入参类型
     * @param <T>              Function 回调返回类型
     * @return
     */
    public <F, T> T supporter(JobRedisCacheKey jobRedisCacheKey, Function<F, T> execute, F input);


}
