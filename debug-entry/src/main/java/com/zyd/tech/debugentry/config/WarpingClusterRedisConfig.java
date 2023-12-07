package com.zyd.tech.debugentry.config;

import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.cache.annotation.EnableCaching;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;
import org.springframework.data.redis.connection.RedisClusterConfiguration;
import org.springframework.data.redis.connection.RedisNode;
import org.springframework.data.redis.connection.jedis.JedisClientConfiguration;
import org.springframework.data.redis.connection.jedis.JedisConnectionFactory;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.StringRedisTemplate;
import redis.clients.jedis.JedisPoolConfig;

import java.time.Duration;
import java.util.HashSet;
import java.util.Set;

@EnableCaching
public class WarpingClusterRedisConfig {
	// defalut库
	@Value("${spring.redis.cluster.nodes}")
	String clusterNodes;
	@Value("${spring.redis.password}")
	String password;

	@Value("${spring.redis.timeout}")
	long timeout;
	@Value("${spring.redis.pool.max-total}")
	int maxTotal;
	@Value("${spring.redis.pool.max-wait}")
	int maxWaitMillis;
	@Value("${spring.redis.pool.max-idle}")
	int maxIdle;
	@Value("${spring.redis.pool.min-idle}")
	int minIdle;
	@Value("${spring.redis.pool.numTestsPerEvictionRun}")
	int numTestsPerEvictionRun;
	@Value("${spring.redis.pool.timeBetweenEvictionRunsMillis}")
	long timeBetweenEvictionRunsMillis;
	@Value("${spring.redis.pool.testWhileIdle}")
	boolean testWhileIdle;
	@Value("${spring.redis.pool.testOnBorrow}")
	boolean testOnBorrow;

	@Primary
	@Bean(name = "warpingRedisTemplate")
	public RedisTemplate<String, Object> redisClusterConfiguration() {
		RedisClusterConfiguration redisClusterConfiguration = new RedisClusterConfiguration();
		// Set<RedisNode> clusterNodes
		String[] serverArray = clusterNodes.split(",");
		Set<RedisNode> nodes = new HashSet<RedisNode>();
		for (String ipPort : serverArray) {
			String[] ipAndPort = ipPort.split(":");
			nodes.add(new RedisNode(ipAndPort[0].trim(), Integer.valueOf(ipAndPort[1])));
		}
		redisClusterConfiguration.setClusterNodes(nodes);
		if (StringUtils.isNotEmpty(password)) {
			redisClusterConfiguration.setPassword(password);
		}
		// 集群模式// new JedisPoolConfig();
		JedisConnectionFactory factory = new JedisConnectionFactory(redisClusterConfiguration, poolCofig());
		// factory.setUsePool(true);
		factory.afterPropertiesSet();
		// 实例化redistemplate
		RedisTemplate<String, Object> redisTemplate = new RedisTemplate<>();
		redisTemplate.setConnectionFactory(factory);
		return redisTemplate;
	}

	@Primary
	@Bean(name = "warpingStringRedisTemplate")
	public StringRedisTemplate redisStringClusterConfiguration() {
		RedisClusterConfiguration redisClusterConfiguration = new RedisClusterConfiguration();
		// Set<RedisNode> clusterNodes
		String[] serverArray = clusterNodes.split(",");
		Set<RedisNode> nodes = new HashSet<RedisNode>();
		for (String ipPort : serverArray) {
			String[] ipAndPort = ipPort.split(":");
			nodes.add(new RedisNode(ipAndPort[0].trim(), Integer.valueOf(ipAndPort[1])));
		}
		redisClusterConfiguration.setClusterNodes(nodes);
		if (StringUtils.isNotEmpty(password)) {
			redisClusterConfiguration.setPassword(password);
		}
		// 集群模式
		JedisConnectionFactory factory = new JedisConnectionFactory(redisClusterConfiguration, poolCofig());// new
																											// JedisPoolConfig()
																											// );
		// factory.setUsePool(true);
		factory.afterPropertiesSet();
		// 实例化redistemplate
		StringRedisTemplate stringRedisTemplate = new StringRedisTemplate();
		stringRedisTemplate.setConnectionFactory(factory);
		return stringRedisTemplate;
	}

	public JedisClientConfiguration poolCofig() {
		JedisClientConfiguration.DefaultJedisClientConfigurationBuilder jedisClientConfiguration = (JedisClientConfiguration.DefaultJedisClientConfigurationBuilder) JedisClientConfiguration
				.builder();
		jedisClientConfiguration.connectTimeout(Duration.ofSeconds(timeout));
		JedisPoolConfig poolCofig = new JedisPoolConfig();
		poolCofig.setMaxIdle(maxIdle);
		poolCofig.setMaxTotal(maxTotal);
		poolCofig.setMaxWaitMillis(maxWaitMillis);
		poolCofig.setTestOnBorrow(testOnBorrow);
		poolCofig.setTimeBetweenEvictionRunsMillis(timeBetweenEvictionRunsMillis);
		poolCofig.setNumTestsPerEvictionRun(numTestsPerEvictionRun);
		jedisClientConfiguration.poolConfig(poolCofig);
		return jedisClientConfiguration.build();
	}

}
