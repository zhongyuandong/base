package com.zyd.tech.catalog.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.io.Serializable;
import java.util.Properties;

public class ProperUtil implements Serializable {
    private static final Logger logger = LoggerFactory.getLogger(ProperUtil.class);
    
    private static String param1;
    private static String param2;
   private static Properties prop;
    public ProperUtil()  {
         prop = new Properties();
//       InputStreamReader inputStreamReader=null;
       try {
//           inputStreamReader = new InputStreamReader(new FileInputStream("redis.properties"), "UTF-8");
           InputStream resourceAsStream = this.getClass().getClassLoader().getResourceAsStream("redis.properties");
           InputStream kafkaStream = this.getClass().getClassLoader().getResourceAsStream("qa-kafka.properties");
           prop.load(resourceAsStream);
           prop.load(kafkaStream);
        } catch (Exception e) {
            logger.error("",e);
        }
    }
    public ProperUtil(String config)  {
        prop = new Properties();
//       InputStreamReader inputStreamReader=null;
        try {
//           inputStreamReader = new InputStreamReader(new FileInputStream("redis.properties"), "UTF-8");
            InputStream resourceAsStream = this.getClass().getClassLoader().getResourceAsStream(config+"-redis.properties");
            InputStream resourceAsredisStream = this.getClass().getClassLoader().getResourceAsStream(config+"-iot-redis.properties");
            InputStream kafkaStream = this.getClass().getClassLoader().getResourceAsStream(config+"-kafka.properties");
            prop.load(resourceAsStream);
            prop.load(resourceAsredisStream);
            prop.load(kafkaStream);
        } catch (Exception e) {
            logger.error("",e);
        }
    }
    public String get(String name) {

        String property = prop.getProperty(name);
        return property;
    }

    public static void main(String[] args) throws Exception {
        ProperUtil pro = new ProperUtil("id");
        String param1 = pro.get("efficiency_savepoint_path");
        System.out.println( param1);
    }
}
