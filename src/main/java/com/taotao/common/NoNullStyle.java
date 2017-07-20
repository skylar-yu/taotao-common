package com.taotao.common;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.lang.reflect.Array;
import java.lang.reflect.Method;
import java.text.SimpleDateFormat;
import java.util.Collection;
import java.util.Date;
import java.util.Map;

/**
 * 功能描述：重写ToStringStyle，不打印出对象的空属性字段
 * <p/>
 * null 不打印
 * “” 打印
 */
public class NoNullStyle extends ToStringStyle implements Serializable {

    private static final long serialVersionUID = 2347542971151578670L;

    /**
     * sl4j
     */
    private static final Logger LOGGER = LoggerFactory.getLogger(NoNullStyle.class);

    @Override
    public void append(StringBuffer buffer, String fieldName, Object value, Boolean fullDetail) {
        if (value != null) {
            super.append(buffer, fieldName, value, fullDetail);
        }

        if (value instanceof String) {
            if (StringUtils.isEmpty((String) value)) {
                return;
            }
        } else if (value instanceof Date) {
            value = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(value);
        }

        // 处理数组, 集合, map
        value = format(value);
        if (value == null) {
            return;
        }

        super.append(buffer, fieldName, value, fullDetail);
    }

    /**
     * 格式化数组集合和map
     *
     * @param value
     * @return
     */
    private Object format(Object value) {
        StringBuilder sb = new StringBuilder();
        format(value, sb);
        return sb.length() == 0 ? null : sb;
    }

    private void format(Object object, StringBuilder sb) {
        if (object == null) {
            sb.append("null");
            return;
        }

        if (object instanceof Map<?, ?>) {
            formatMap((Map<?, ?>) object, sb);
        } else if (object instanceof Iterable<?>) {
            formatCollection((Collection<?>) object, sb);
        } else if (object.getClass().isArray()) {
            formatArray(object, sb);
        } else if (object.getClass() == Object.class) {
            sb.append(object);
        } else {
            // 针对没有重载Object.toString()方法的对象
            Method toStringMethod = null;
            Class<? extends Object> clazz = object.getClass();
            try {
                toStringMethod = clazz.getMethod("toString");
            } catch (Exception e) {
                LOGGER.error("指定的类" + clazz.getName() + "不存在toString()方法");
            }
            if (toStringMethod == null || toStringMethod.getDeclaringClass() == Object.class) {
                object = ToStringBuilder.reflectionToString(object);
            }
            sb.append(object);
        }
    }

    private <K, V> void formatMap(Map<K, V> map, StringBuilder sb) {
        if (((Map<?, ?>) map).size() == 0) {
            return;
        }

        boolean first = true;
        sb.append('{');
        for (Map.Entry<K, V> entry : map.entrySet()) {
            if (first) {
                first = false;
            } else {
                sb.append(',');
            }
            K key = entry.getKey();
            V value = entry.getValue();
            sb.append(key).append('=');
            format(value, sb);
        }
        sb.append('}');
    }

    private void formatArray(Object array, StringBuilder sb) {
        if (Array.getLength(array) == 0) {
            return;
        }

        int length = Array.getLength(array);
        sb.append('[');
        for (int i = 0; i < length; ++i) {
            if (i > 0) {
                sb.append(',');
            }
            Object object = Array.get(array, i);
            format(object, sb);
        }
        sb.append(']');
    }

    private <T> void formatCollection(Collection<T> collection, StringBuilder sb) {
        if (collection.size() == 0) {
            return;
        }

        boolean first = true;
        sb.append('[');
        for (T t : collection) {
            if (first) {
                first = false;
            } else {
                sb.append(',');
            }
            format(t, sb);
        }
        sb.append(']');
    }
}
