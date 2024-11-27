package com.bigdata.it4931.utility;

import lombok.experimental.UtilityClass;

import java.util.Objects;

@UtilityClass
public class StringUtils {
    public boolean isNullOrEmpty(String content) {
        return content == null || content.isEmpty();
    }

    public String replace(String str, String regex, String replacement) {
        if (Objects.isNull(str)) {
            return null;
        }
        return str.replaceAll(regex, replacement);
    }
}
