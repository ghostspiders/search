package org.server.search.env;

import cn.hutool.core.util.ObjectUtil;
import cn.hutool.core.util.StrUtil;

public class Profile {
    public enum Type {
        DEV,
        TEST,
        UAT,
        PROD
    }

    public static String value(Type type) {
        if(ObjectUtil.isNull(type)){
            return "";
        } else if (Type.DEV.equals(type)) {
            return "dev";
        } else if (Type.TEST.equals(type)) {
            return "test";
        } else if (Type.UAT.equals(type)) {
            return "uat";
        } else if (Type.PROD.equals(type)) {
            return "prod";
        } else {
            return "";
        }
    }
    public static Type readVale(String type) {
        if(StrUtil.isBlank(type)){
            return null;
        } else if ("dev".equalsIgnoreCase(type)) {
            return Type.DEV;
        } else if ("test".equalsIgnoreCase(type)) {
            return Type.TEST;
        } else if ("uat".equalsIgnoreCase(type)) {
            return Type.UAT;
        } else if ("prod".equalsIgnoreCase(type)) {
            return Type.PROD;
        } else {
            return null;
        }
    }
}
