package com.qqlei.shop.dataaggr.rabbitmq;

import org.springframework.amqp.rabbit.annotation.RabbitHandler;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import redis.clients.jedis.JedisCluster;

import com.alibaba.fastjson.JSONObject;

import java.util.List;

@Component  
@RabbitListener(queues = "aggr-data-change-queue")  
public class AggrDataChangeQueueReceiver {  
	
	@Autowired
	private JedisCluster jedisCluster;
	
    @RabbitHandler  
    public void process(String message) {  
    	JSONObject messageJSONObject = JSONObject.parseObject(message);
		System.out.println("================================聚合服务收到消息【"+message+"】========================================");
    	String dimType = messageJSONObject.getString("dim_type");  
    	
    	if("brand".equals(dimType)) {
    		processBrandDimDataChange(messageJSONObject); 
    	} else if("category".equals(dimType)) {
    		processCategoryDimDataChange(messageJSONObject); 
    	} else if("product_intro".equals(dimType)) {
    		processProductIntroDimDataChange(messageJSONObject); 
    	} else if("product".equals(dimType)) {
    		processProductDimDataChange(messageJSONObject); 
    	}
    }  
    
    private void processBrandDimDataChange(JSONObject messageJSONObject) {
    	Long id = messageJSONObject.getLong("id");
    	
    	// 多此一举，看一下，查出来一个品牌数据，然后直接就原样写redis
    	// 实际上是这样子的，我们这里是简化了数据结构和业务，实际上任何一个维度数据都不可能只有一个原子数据
    	// 品牌数据，肯定是结构多变的，结构比较复杂，有很多不同的表，不同的原子数据
    	// 实际上这里肯定是要将一个品牌对应的多个原子数据都从redis查询出来，然后聚合之后写入redis
    	String dataJSON = jedisCluster.get("brand_" + id);
    	
    	if(dataJSON != null && !"".equals(dataJSON)) {
			jedisCluster.set("dim_brand_" + id, dataJSON);
    	} else {
			jedisCluster.del("dim_brand_" + id);
    	}
    }
    
    private void processCategoryDimDataChange(JSONObject messageJSONObject) {
    	Long id = messageJSONObject.getLong("id");
    	
    	// 多此一举，看一下，查出来一个品牌数据，然后直接就原样写redis
    	// 实际上是这样子的，我们这里是简化了数据结构和业务，实际上任何一个维度数据都不可能只有一个原子数据
    	// 品牌数据，肯定是结构多变的，结构比较复杂，有很多不同的表，不同的原子数据
    	// 实际上这里肯定是要将一个品牌对应的多个原子数据都从redis查询出来，然后聚合之后写入redis
    	String dataJSON = jedisCluster.get("category_" + id);
    	
    	if(dataJSON != null && !"".equals(dataJSON)) {
			jedisCluster.set("dim_category_" + id, dataJSON);
    	} else {
			jedisCluster.del("dim_category_" + id);
    	}
    }
    
    private void processProductIntroDimDataChange(JSONObject messageJSONObject) {
    	Long id = messageJSONObject.getLong("id");
    	
    	String dataJSON = jedisCluster.get("product_intro_" + id);
    	
    	if(dataJSON != null && !"".equals(dataJSON)) {
			jedisCluster.set("dim_product_intro_" + id, dataJSON);
    	} else {
			jedisCluster.del("dim_product_intro_" + id);
    	}
    }
    
    private void processProductDimDataChange(JSONObject messageJSONObject) {
    	Long id = messageJSONObject.getLong("id");
		//redis mget批量查询数据 ,一次从redis 中获取多条数据
//		List<String> results  = jedisCluster.mget("product_" + id, "product_property_" + id, "product_specification_" + id);
//		String productDataJSON = results.get(0);

    	String productDataJSON = jedisCluster.get("product_" + id);
    	
    	if(productDataJSON != null && !"".equals(productDataJSON)) {
    		JSONObject productDataJSONObject = JSONObject.parseObject(productDataJSON);
//			String productPropertyDataJSON = results.get(1);
    		String productPropertyDataJSON = jedisCluster.get("product_property_" + id);
    		if(productPropertyDataJSON != null && !"".equals(productPropertyDataJSON)) {
    			productDataJSONObject.put("product_property", JSONObject.parse(productPropertyDataJSON));
    		}
//			String productSpecificationDataJSON = results.get(2);
    		String productSpecificationDataJSON = jedisCluster.get("product_specification_" + id);
    		if(productSpecificationDataJSON != null && !"".equals(productSpecificationDataJSON)) {
    			productDataJSONObject.put("product_specification", JSONObject.parse(productSpecificationDataJSON));
    		}

			jedisCluster.set("dim_product_" + id, productDataJSONObject.toJSONString());
    	} else {
			jedisCluster.del("dim_product_" + id);
    	}
    }
  
}  