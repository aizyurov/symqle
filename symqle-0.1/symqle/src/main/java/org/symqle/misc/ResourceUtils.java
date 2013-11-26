package org.symqle.misc;

import org.symqle.common.Bug;

import java.io.InputStream;
import java.util.Properties;

/**
 * @author lvovich
 */
public class ResourceUtils {

    private ResourceUtils() {}

    static { new ResourceUtils(); }

    public static Properties readProperties(final String path) {
        final Properties properties = new Properties();
        new SafeCallable() {
            @Override
            public Void call() throws Exception {
                final InputStream inputStream = ResourceUtils.class.getClassLoader().getResourceAsStream(path);
                try {
                    Bug.reportIfNull(inputStream);
                    properties.load(inputStream);
                } finally {
                    inputStream.close();
                }
                return null;
            }
        }.run();
        return properties;
    }

}
