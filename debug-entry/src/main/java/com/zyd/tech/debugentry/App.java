package com.zyd.tech.debugentry;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * Hello world!
 *
 */
@ComponentScan(basePackages = { "com.zyd.tech.debugentry" }) // 扫描该包路径下的所有spring组件
@SpringBootApplication
//@EnableScheduling
@Configuration
@EnableAsync
public class App 
{
    public static void main( String[] args )
    {
        SpringApplication.run(App.class, args);
    }
}
