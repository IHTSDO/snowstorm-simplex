package org.snomed.snowstorm.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jms.config.DefaultJmsListenerContainerFactory;

import jakarta.jms.ConnectionFactory;

@Configuration
public class JMSConfig {
    
	private final ConnectionFactory connectionFactory;

	public JMSConfig(ConnectionFactory connectionFactory) {
		this.connectionFactory = connectionFactory;
	}

	@Bean(name = "topicJmsListenerContainerFactory")
	public DefaultJmsListenerContainerFactory getTopicFactory() {
		DefaultJmsListenerContainerFactory factory = new  DefaultJmsListenerContainerFactory();
		factory.setConnectionFactory(connectionFactory);
		factory.setSessionTransacted(true);
		factory.setPubSubDomain(true);
		return factory;
	}
}
