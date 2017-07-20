package com.taotao.common.job.impl;

import com.google.common.collect.Sets;
import org.apache.commons.lang.StringUtils;

import java.util.Set;

/**
 * 功能描述：定时任务redis key
 *
 * @author: Zhenbin.Li
 * email： lizhenbin@oneplus.cn
 * company：一加科技
 * Date: 16/7/14 Time: 09:57
 */
public enum JobRedisCacheKey {

    /**
     * 内销订单推送到第三方KEY
     */
    SYN_LOCAL_ORDER_TO_TPL("SYN_LOCAL_ORDER_TO_TPL", "TRUE", 10 * 60),

    /**
     * 定时任务抓取4px订单KEY
     */
    SYN_GET_ORDER_STATUS_FROM_FPX("SYN_GET_ORDER_STATUS_FROM_FPX", "TRUE", 10 * 60),

    /**
     * 费用主动抓取订单KEY
     */
    FETCH_ORDER_TO_BASE_FEE_VOUCHER("FETCH_ORDER_TO_BASE_FEE_VOUCHER", "TRUE", 10 * 60),

    /**
     * 计算计费单KEY
     */
    FETCH_ORDER_CALCULATE("FETCH_ORDER_CALCULATE", "TRUE", 10 * 60),

    /**
     * SCM推送中间表数据到ERP KEY
     */
    SCM_PUSH_ERP_DATA("SCM_PUSH_ERP_DATA", "TRUE", 10 * 60),

    /**
     * SCM向内部其他系统推送订单状态KEY
     */
    SCM_PUSH_ORDER_STATUS("SCM_PUSH_ORDER_STATUS", "TRUE", 10 * 60),

    /**
     * 推送订单到BD
     */
    PUSH_SO_TO_BD("PUSH_SO_TO_BD", "TRUE", 10 * 60),

    /**
     * 查询BD订单状态
     */
    FETCH_SO_FROM_BD("FETCH_SO_FROM_BD", "TRUE", 10 * 60);

    /**
     * KEY
     */
    private String key;

    /**
     * value
     */
    private String value;

    /**
     * 超时时间
     */
    private int delayTime;

    JobRedisCacheKey(String key, String value, int delayTime) {
        this.key = key;
        this.value = value;
        this.delayTime = delayTime;
    }

    public String getKey() {
        checkKey();
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public int getDelayTime() {
        return delayTime;
    }

    public void setDelayTime(int delayTime) {
        this.delayTime = delayTime;
    }

    public static JobRedisCacheKey getByType(String typeCode) {
        if (StringUtils.isEmpty(typeCode)) {
            return null;
        }

        for (JobRedisCacheKey type : JobRedisCacheKey.values()) {
            if (type.getKey().equals(typeCode)) {
                return type;
            }
        }
        return null;
    }

    /**
     * 校验KEY是否重复
     */
    private void checkKey() {
        Set<String> valueSet = Sets.newHashSet();
        for (JobRedisCacheKey jobRedisCacheKey : JobRedisCacheKey.values()) {
            valueSet.add(jobRedisCacheKey.key);
        }
        if (valueSet.size() != JobRedisCacheKey.values().length) {
            throw new RuntimeException("JobRedisCacheKey存在重复的KEY!");
        }
    }
}
