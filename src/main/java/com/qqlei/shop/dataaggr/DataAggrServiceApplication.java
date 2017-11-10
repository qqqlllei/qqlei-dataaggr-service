package com.qqlei.shop.dataaggr;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.netflix.eureka.EnableEurekaClient;
import org.springframework.cloud.netflix.feign.EnableFeignClients;
import org.springframework.context.annotation.Bean;

import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;

import java.util.HashSet;
import java.util.Set;

@SpringBootApplication
@EnableEurekaClient
@EnableFeignClients
public class DataAggrServiceApplication {

	public static void main(String[] args) {
		SpringApplication.run(DataAggrServiceApplication.class, args);
	}

	@Bean
	public JedisCluster JedisClusterFactory() {
		Set<HostAndPort> jedisClusterNodes = new HashSet<>();
		jedisClusterNodes.add(new HostAndPort("10.33.80.104", 6379));
		jedisClusterNodes.add(new HostAndPort("10.33.80.105", 6379));
		jedisClusterNodes.add(new HostAndPort("10.33.80.106", 6379));
		JedisCluster jedisCluster = new JedisCluster(jedisClusterNodes);
		return jedisCluster;
	}
	
}
