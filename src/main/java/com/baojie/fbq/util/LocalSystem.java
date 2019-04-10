package com.baojie.fbq.util;

public class LocalSystem {

    public static final String JAVA_SPECIFICATION_VERSION = getSystemProperty("java.specification.version");

    private static final String getSystemProperty(final String property) {
        try {
            return System.getProperty(property);
        } catch (final SecurityException ex) {
            // we are not allowed to look at this property
            // System.err.println("Caught a SecurityException reading the system property '" + property
            // + "'; the SystemUtils property value will default to null.");
            return null;
        }
    }

    private static final JavaVersion JAVA_SPECIFICATION_VERSION_AS_ENUM = JavaVersion.get(JAVA_SPECIFICATION_VERSION);

    public static final boolean isJavaVersionAtLeast(JavaVersion requiredVersion) {
        return JAVA_SPECIFICATION_VERSION_AS_ENUM.atLeast(requiredVersion);
    }

}
