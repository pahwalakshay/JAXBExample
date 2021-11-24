package com.mediaocean.bi.cmdw.sync.mmp.amqp;

import com.mediaocean.bi.cmdw.sync.mmp.PlanMessageConsumer;
import com.mediaocean.bi.cmdw.sync.mmp.amqp.listeners.LPMFieldValueUpdateListener;
import com.mediaocean.bi.cmdw.sync.mmp.amqp.listeners.MMPObjectUpdateListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.*;
import org.springframework.amqp.rabbit.annotation.RabbitListenerConfigurer;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.listener.RabbitListenerEndpointRegistrar;
import org.springframework.amqp.rabbit.listener.SimpleMessageListenerContainer;
import org.springframework.amqp.rabbit.listener.adapter.MessageListenerAdapter;
import org.springframework.amqp.support.converter.Jackson2JsonMessageConverter;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.web.support.SpringBootServletInitializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;
import org.springframework.context.annotation.Profile;
import org.springframework.messaging.converter.MappingJackson2MessageConverter;
import org.springframework.messaging.handler.annotation.support.DefaultMessageHandlerMethodFactory;
import org.springframework.retry.policy.SimpleRetryPolicy;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;

@Component
@Profile("docker")
public class RabbitConfiguration extends SpringBootServletInitializer implements RabbitListenerConfigurer {

    public static final String SITE_EVENT_KEY = "site-event";
    public static final String EVENTS_EXCHANGE_NAME = "${rabbit.mmp-etl.events.exchange}";
    //	public static final String PLAN_PROCESS_SCALING = "${rabbit.mmp-etl.plan.queue.process.scaling:5}";
    public static final String PLAN_THREAD_SCALING = "${rabbit.mmp-etl.plan.queue.thread.scaling:10}";
    public static final String PLAN_THREAD_SCALING_INCREMENTAL = "${rabbit.mmp-etl.plan.queue.thread.scaling.incremental:5}";
    public static final String PLAN_THREAD_MAX_SCALING = "${rabbit.mmp-etl.plan.queue.thread.max-scaling:20}";
    public static final String PLAN_QUEUE_RK_PREFIX = "${rabbit.mmp-etl.plan.queue.rk.prefix}";
    public static final String PLAN_QUEUE_RK_PREFIX_INCREMENTAL = "${rabbit.mmp-etl.plan.queue.rk.prefix.incremental}";
    public static final String PLAN_QUEUE_NAME_PREFIX = "${rabbit.mmp-etl.plan.queue.prefix}";
    public static final String PLAN_QUEUE_NAME_PREFIX_INCREMENTAL = "${rabbit.mmp-etl.plan.queue.prefix.incremental}";
    public static final String PLAN_EXCHANGE_PROP_NAME = "${rabbit.mmp-etl.plan.exchange}";
    public static final String PLAN_EXCHANGE_PROP_NAME_INCREMENTAL = "${rabbit.mmp-etl.plan.exchange.incremental}";

    public static final String LPM_PLAN_PROCESS_LISTENER_SCALING = "${rabbit.mmp-etl.plan.process.listner.thread.scaling:20}";
    private static final Logger LOGGER = LoggerFactory.getLogger(RabbitConfiguration.class);

    @Value(RabbitConfiguration.LPM_PLAN_PROCESS_LISTENER_SCALING)
    private int lpmPlanProcessListenerScaling;


    @Value("${rabbit.mmp-etl.exchange}")
    private String rabbitExchange;
    @Value("${rabbit.mmp-etl.config.queue}")
    private String rabbitQueue;
    @Value("${rabbit.mmp-etl.config.queue.rk}")
    private String rabbitRoutingKey;
    @Value("${rabbit.mmp-etl.config.max.attempts}")
    private int maxAttempts;
    @Value("${spring.rabbitmq.host}")
    private String rabbitHost;
    @Value("${spring.rabbitmq.username}")
    private String rabbitUser;
    @Value("${spring.rabbitmq.password}")
    private String rabbitPass;
    @Value("${spring.rabbitmq.requested.heartbeat:10}")
    private int rabbitHeartbeatSeconds;
    @Value("${spring.rabbitmq.port:5672}")
    private int rabbitmqPort;
    @Value("${rabbit.lpm.exchange}")
    private String rabbitLPMExchange;
    @Value("${rabbit.lpm.config.queue}")
    private String rabbitLPMQueue;
    @Value("${rabbit.lpm.config.queue.rk}")
    private String rabbitLPMRoutingKey;
    @Value("${rabbit.currency.exchange}")
    private String rabbitCurrencyExchange;
    @Value("${rabbit.currency.config.queue}")
    private String rabbitCurrencyQueue;
    @Value("${rabbit.currency.config.queue.rk}")
    private String rabbitCurrencyRoutingKey;
    @Value("${rabbit.lpm.config.mediaplan.update.queue}")
    private String rabbitLPMMediaPlanUpdateQueue;
    @Value("${rabbit.lpm.config.mediaplan.update.queue.rk}")
    private String rabbitLPMMediaPlanUpdateRoutingKey;
    @Value(RabbitConfiguration.PLAN_EXCHANGE_PROP_NAME)
    private String rabbitETLPlanDirectExchange;
    @Value(RabbitConfiguration.PLAN_EXCHANGE_PROP_NAME_INCREMENTAL)
    private String rabbitETLIncrementalPlanDirectExchange;
    @Value(RabbitConfiguration.PLAN_QUEUE_NAME_PREFIX)
    private String rabbitETLPlanQueuePrefix;
    @Value(RabbitConfiguration.PLAN_QUEUE_NAME_PREFIX_INCREMENTAL)
    private String rabbitETLPlanIncrementalQueuePrefix;
    @Value(RabbitConfiguration.PLAN_QUEUE_RK_PREFIX)
    private String rabbitETLPlanRKPrefix;
    @Value(RabbitConfiguration.PLAN_QUEUE_RK_PREFIX_INCREMENTAL)
    private String rabbitETLIncrementalPlanRKPrefix;
    @Value("${rabbit.mmp-etl.plan.queue.concurrency:1}")
    private int perQueueThreadPoolSize;

    @Value("${rabbit.lpm.config.fieldvalue.update.queue}")
    private String rabbitLPMFieldValueUpdateQueue;
    @Value("${rabbit.lpm.config.fieldvalue.update.queue.rk}")
    private String rabbitLPMFieldValueUpdateRoutingKey;


    @Value("${rabbit.mmp.object.updates.exchange}")
    private String rabbiMMPObjectUpdateExchange;
    @Value("${rabbit.mmp.config.fieldvalue.update.queue}")
    private String rabbiMMPObjectUpdateMQueue;
    @Value("${rabbit.mmp.config.fieldvalue.update.queue.rk}")
    private String rabbiMMPObjectUpdateRoutingKey;






    @Bean
    public TopicExchange getRabbitExchange() {
        return new TopicExchange(rabbitExchange);
    }

    @Bean
    public Queue getRabbitQueue() {
        return new Queue(rabbitQueue);
    }

    @Bean
    public Binding declareBindingApp1() {
        return BindingBuilder.bind(getRabbitQueue()).to(getRabbitExchange()).with(rabbitRoutingKey);
    }

    @Bean
    public DirectExchange getRabbitExchangeLPM() {
        return new DirectExchange(rabbitLPMExchange, true, false);
    }

    @Bean
    public Queue getLPMRabbitQueue() {
        return new Queue(rabbitLPMQueue, true, false, false);
    }

    @Bean
    public Binding declareLPMBindingApp(@Qualifier("getLPMRabbitQueue") final Queue lpmqueue,
                                        @Qualifier("getRabbitExchangeLPM") final DirectExchange lpmExchange) {
        Binding binding = BindingBuilder.bind(lpmqueue).to(lpmExchange).with(rabbitLPMRoutingKey);
        return binding;
    }

    @Bean
    public DirectExchange getRabbitExchangeCurrency() {
        return new DirectExchange(rabbitCurrencyExchange, true, false);
    }

    @Bean
    public Queue getCurrencyRabbitQueue() {
        return new Queue(rabbitCurrencyQueue, true, false, false);
    }

    @Bean
    public Binding declareCurrencyBindingApp(@Qualifier("getCurrencyRabbitQueue") final Queue currencyQueue,
                                             @Qualifier("getRabbitExchangeCurrency") final DirectExchange currencyExchange) {
        Binding binding = BindingBuilder.bind(currencyQueue).to(currencyExchange).with(rabbitCurrencyRoutingKey);
        return binding;
    }

    @Bean(name = "amqpAdmin")
    public AmqpAdmin amqpAdmin(@Qualifier("getAmqpConnectionFactory") final ConnectionFactory connectionFactory) {
        AmqpAdmin admin = new RabbitAdmin(connectionFactory);
        return admin;
    }

    @Bean
    ConnectionFactory getAmqpConnectionFactory() {
        CachingConnectionFactory connectionFactory = new CachingConnectionFactory(rabbitHost, rabbitmqPort);
        LOGGER.info("Creating AMQP connection factory");
        if (!rabbitUser.isEmpty()) {
            connectionFactory.setUsername(rabbitUser);
            LOGGER.info("Logging to AMQP with username ", rabbitUser);
        } else {
            LOGGER.warn("Logging to AMQP with NO username");
        }
        if (!rabbitPass.isEmpty()) {
            connectionFactory.setPassword(rabbitPass);
            LOGGER.info("Logging to AMQP with password {}", rabbitPass.replaceAll(".", "*"));
        } else {
            LOGGER.warn("Logging to AMQP with NO password");
        }
        connectionFactory.setRequestedHeartBeat(rabbitHeartbeatSeconds);
        return connectionFactory;
    }

    @Bean
    public RabbitTemplate rabbitTemplate() {
        final RabbitTemplate rabbitTemplate = new RabbitTemplate(getAmqpConnectionFactory());
        rabbitTemplate.setMessageConverter(producerJackson2MessageConverter());
        rabbitTemplate.setRetryTemplate(retryTemplate());
        return rabbitTemplate;
    }

    private RetryTemplate retryTemplate() {
        RetryTemplate retryTemplate = new RetryTemplate();
        retryTemplate.setRetryPolicy(retryPolicy());
        return retryTemplate;
    }

    private SimpleRetryPolicy retryPolicy() {
        Map<Class<? extends Throwable>, Boolean> retryExceptionMap = new HashMap<>();
        retryExceptionMap.put(ETLConfigRetryException.class, true);

        SimpleRetryPolicy retryPolicy = new SimpleRetryPolicy(maxAttempts, retryExceptionMap);
        return retryPolicy;
    }

    @Bean
    public Jackson2JsonMessageConverter producerJackson2MessageConverter() {
        return new Jackson2JsonMessageConverter();
    }

    @Bean
    public MappingJackson2MessageConverter consumerJackson2MessageConverter() {
        return new MappingJackson2MessageConverter();
    }

    @Bean
    public DefaultMessageHandlerMethodFactory messageHandlerMethodFactory() {
        DefaultMessageHandlerMethodFactory factory = new DefaultMessageHandlerMethodFactory();
        factory.setMessageConverter(consumerJackson2MessageConverter());
        return factory;
    }

    @Override
    public void configureRabbitListeners(final RabbitListenerEndpointRegistrar registrar) {
        registrar.setMessageHandlerMethodFactory(messageHandlerMethodFactory());
    }

    @Bean
    public DirectExchange rabbitExchangePlanProcessing() {
        return new DirectExchange(rabbitETLPlanDirectExchange, true, false);
    }

    @Bean
    public DirectExchange rabbitExchangeIncrementalPlanProcessing() {
        return new DirectExchange(rabbitETLIncrementalPlanDirectExchange, true, false);
    }

    @Profile("background")
    @Bean
    public Queue queue1(PlanQueueConfiguration queueConfig) {
        return queueConfig.getPlanQueues().get(0).getQueue();
    }

    @Profile("background")
    @Bean
    public Queue queue2(PlanQueueConfiguration queueConfig) {
        return queueConfig.getPlanQueues().get(1).getQueue();
    }

    @Profile("background")
    @Bean
    public Queue queue3(PlanQueueConfiguration queueConfig) {
        return queueConfig.getPlanQueues().get(2).getQueue();
    }

    @Profile("background")
    @Bean
    public Queue queue4(PlanQueueConfiguration queueConfig) {
        return queueConfig.getPlanQueues().get(3).getQueue();
    }

    @Profile("background")
    @Bean
    public Queue queue5(PlanQueueConfiguration queueConfig) {
        return queueConfig.getPlanQueues().get(4).getQueue();
    }

    @Profile("background")
    @Bean
    public Queue queue6(PlanQueueConfiguration queueConfig) {
        return queueConfig.getPlanQueues().get(5).getQueue();
    }

    @Profile("background")
    @Bean
    public Queue queue7(PlanQueueConfiguration queueConfig) {
        return queueConfig.getPlanQueues().get(6).getQueue();
    }

    @Profile("background")
    @Bean
    public Queue queue8(PlanQueueConfiguration queueConfig) {
        return queueConfig.getPlanQueues().get(7).getQueue();
    }

    @Profile("background")
    @Bean
    public Queue queue9(PlanQueueConfiguration queueConfig) {
        return queueConfig.getPlanQueues().get(8).getQueue();
    }

    @Profile("background")
    @Bean
    public Queue queue10(PlanQueueConfiguration queueConfig) {
        return queueConfig.getPlanQueues().get(9).getQueue();
    }

    @Profile("background")
    @Bean
    public Queue queue11(PlanQueueConfiguration queueConfig) {
        return queueConfig.getPlanQueues().get(10).getQueue();
    }

    @Profile("background")
    @Bean
    public Queue queue12(PlanQueueConfiguration queueConfig) {
        return queueConfig.getPlanQueues().get(11).getQueue();
    }

    @Profile("background")
    @Bean
    public Queue queue13(PlanQueueConfiguration queueConfig) {
        return queueConfig.getPlanQueues().get(12).getQueue();
    }

    @Profile("background")
    @Bean
    public Queue queue14(PlanQueueConfiguration queueConfig) {
        return queueConfig.getPlanQueues().get(13).getQueue();
    }

    @Profile("background")
    @Bean
    public Queue queue15(PlanQueueConfiguration queueConfig) {
        return queueConfig.getPlanQueues().get(14).getQueue();
    }

    @Profile("background")
    @Bean
    public Queue queue16(PlanQueueConfiguration queueConfig) {
        return queueConfig.getPlanQueues().get(15).getQueue();
    }

    @Profile("background")
    @Bean
    public Queue queue17(PlanQueueConfiguration queueConfig) {
        return queueConfig.getPlanQueues().get(16).getQueue();
    }

    @Profile("background")
    @Bean
    public Queue queue18(PlanQueueConfiguration queueConfig) {
        return queueConfig.getPlanQueues().get(17).getQueue();
    }

    @Profile("background")
    @Bean
    public Queue queue19(PlanQueueConfiguration queueConfig) {
        return queueConfig.getPlanQueues().get(18).getQueue();
    }

    @Profile("background")
    @Bean
    public Queue queue20(PlanQueueConfiguration queueConfig) {
        return queueConfig.getPlanQueues().get(19).getQueue();
    }

    @Profile("background")
    @Bean
    public Queue incrementalQueue1(PlanQueueConfiguration queueConfig) {
        return queueConfig.getPlanQueuesForIncremental().get(0).getQueue();
    }

    @Profile("background")
    @Bean
    public Queue incrementalQueue2(PlanQueueConfiguration queueConfig) {
        return queueConfig.getPlanQueuesForIncremental().get(1).getQueue();
    }

    @Profile("background")
    @Bean
    public Queue incrementalQueue3(PlanQueueConfiguration queueConfig) {
        return queueConfig.getPlanQueuesForIncremental().get(2).getQueue();
    }

    @Profile("background")
    @Bean
    public Queue incrementalQueue4(PlanQueueConfiguration queueConfig) {
        return queueConfig.getPlanQueuesForIncremental().get(3).getQueue();
    }

    @Profile("background")
    @Bean
    public Queue incrementalQueue5(PlanQueueConfiguration queueConfig) {
        return queueConfig.getPlanQueuesForIncremental().get(4).getQueue();
    }

    @Profile("background")
    @Bean
    public Binding declarePlanQueueBinding1(@Qualifier("rabbitExchangePlanProcessing") final DirectExchange exchange,
                                            PlanQueueConfiguration queueConfig) {
        return BindingBuilder.bind(queueConfig.getPlanQueues().get(0).getQueue()).to(exchange)
                .with(queueConfig.getPlanQueues().get(0).getRoutingKey());
    }

    @Profile("background")
    @Bean
    public Binding declarePlanQueueBinding2(@Qualifier("rabbitExchangePlanProcessing") final DirectExchange exchange,
                                            PlanQueueConfiguration queueConfig) {
        return BindingBuilder.bind(queueConfig.getPlanQueues().get(1).getQueue()).to(exchange)
                .with(queueConfig.getPlanQueues().get(1).getRoutingKey());
    }

    @Profile("background")
    @Bean
    public Binding declarePlanQueueBinding3(@Qualifier("rabbitExchangePlanProcessing") final DirectExchange exchange,
                                            PlanQueueConfiguration queueConfig) {
        return BindingBuilder.bind(queueConfig.getPlanQueues().get(2).getQueue()).to(exchange)
                .with(queueConfig.getPlanQueues().get(2).getRoutingKey());
    }

    @Profile("background")
    @Bean
    public Binding declarePlanQueueBinding4(@Qualifier("rabbitExchangePlanProcessing") final DirectExchange exchange,
                                            PlanQueueConfiguration queueConfig) {
        return BindingBuilder.bind(queueConfig.getPlanQueues().get(3).getQueue()).to(exchange)
                .with(queueConfig.getPlanQueues().get(3).getRoutingKey());
    }

    @Profile("background")
    @Bean
    public Binding declarePlanQueueBinding5(@Qualifier("rabbitExchangePlanProcessing") final DirectExchange exchange,
                                            PlanQueueConfiguration queueConfig) {
        return BindingBuilder.bind(queueConfig.getPlanQueues().get(4).getQueue()).to(exchange)
                .with(queueConfig.getPlanQueues().get(4).getRoutingKey());
    }

    @Profile("background")
    @Bean
    public Binding declarePlanQueueBinding6(@Qualifier("rabbitExchangePlanProcessing") final DirectExchange exchange,
                                            PlanQueueConfiguration queueConfig) {
        return BindingBuilder.bind(queueConfig.getPlanQueues().get(5).getQueue()).to(exchange)
                .with(queueConfig.getPlanQueues().get(5).getRoutingKey());
    }

    @Profile("background")
    @Bean
    public Binding declarePlanQueueBinding7(@Qualifier("rabbitExchangePlanProcessing") final DirectExchange exchange,
                                            PlanQueueConfiguration queueConfig) {
        return BindingBuilder.bind(queueConfig.getPlanQueues().get(6).getQueue()).to(exchange)
                .with(queueConfig.getPlanQueues().get(6).getRoutingKey());
    }

    @Profile("background")
    @Bean
    public Binding declarePlanQueueBinding8(@Qualifier("rabbitExchangePlanProcessing") final DirectExchange exchange,
                                            PlanQueueConfiguration queueConfig) {
        return BindingBuilder.bind(queueConfig.getPlanQueues().get(7).getQueue()).to(exchange)
                .with(queueConfig.getPlanQueues().get(7).getRoutingKey());
    }

    @Profile("background")
    @Bean
    public Binding declarePlanQueueBinding9(@Qualifier("rabbitExchangePlanProcessing") final DirectExchange exchange,
                                            PlanQueueConfiguration queueConfig) {
        return BindingBuilder.bind(queueConfig.getPlanQueues().get(8).getQueue()).to(exchange)
                .with(queueConfig.getPlanQueues().get(8).getRoutingKey());
    }

    @Profile("background")
    @Bean
    public Binding declarePlanQueueBinding10(@Qualifier("rabbitExchangePlanProcessing") final DirectExchange exchange,
                                             PlanQueueConfiguration queueConfig) {
        return BindingBuilder.bind(queueConfig.getPlanQueues().get(9).getQueue()).to(exchange)
                .with(queueConfig.getPlanQueues().get(9).getRoutingKey());
    }

    @Profile("background")
    @Bean
    public Binding declarePlanQueueBinding11(@Qualifier("rabbitExchangePlanProcessing") final DirectExchange exchange,
                                             PlanQueueConfiguration queueConfig) {
        return BindingBuilder.bind(queueConfig.getPlanQueues().get(10).getQueue()).to(exchange)
                .with(queueConfig.getPlanQueues().get(10).getRoutingKey());
    }

    @Profile("background")
    @Bean
    public Binding declarePlanQueueBinding12(@Qualifier("rabbitExchangePlanProcessing") final DirectExchange exchange,
                                             PlanQueueConfiguration queueConfig) {
        return BindingBuilder.bind(queueConfig.getPlanQueues().get(11).getQueue()).to(exchange)
                .with(queueConfig.getPlanQueues().get(11).getRoutingKey());
    }

    @Profile("background")
    @Bean
    public Binding declarePlanQueueBinding13(@Qualifier("rabbitExchangePlanProcessing") final DirectExchange exchange,
                                             PlanQueueConfiguration queueConfig) {
        return BindingBuilder.bind(queueConfig.getPlanQueues().get(12).getQueue()).to(exchange)
                .with(queueConfig.getPlanQueues().get(12).getRoutingKey());
    }


    @Profile("background")
    @Bean
    public Binding declarePlanQueueBinding14(@Qualifier("rabbitExchangePlanProcessing") final DirectExchange exchange,
                                             PlanQueueConfiguration queueConfig) {
        return BindingBuilder.bind(queueConfig.getPlanQueues().get(13).getQueue()).to(exchange)
                .with(queueConfig.getPlanQueues().get(13).getRoutingKey());
    }


    @Profile("background")
    @Bean
    public Binding declarePlanQueueBinding15(@Qualifier("rabbitExchangePlanProcessing") final DirectExchange exchange,
                                             PlanQueueConfiguration queueConfig) {
        return BindingBuilder.bind(queueConfig.getPlanQueues().get(14).getQueue()).to(exchange)
                .with(queueConfig.getPlanQueues().get(14).getRoutingKey());
    }


    @Profile("background")
    @Bean
    public Binding declarePlanQueueBinding16(@Qualifier("rabbitExchangePlanProcessing") final DirectExchange exchange,
                                             PlanQueueConfiguration queueConfig) {
        return BindingBuilder.bind(queueConfig.getPlanQueues().get(15).getQueue()).to(exchange)
                .with(queueConfig.getPlanQueues().get(15).getRoutingKey());
    }


    @Profile("background")
    @Bean
    public Binding declarePlanQueueBinding17(@Qualifier("rabbitExchangePlanProcessing") final DirectExchange exchange,
                                             PlanQueueConfiguration queueConfig) {
        return BindingBuilder.bind(queueConfig.getPlanQueues().get(16).getQueue()).to(exchange)
                .with(queueConfig.getPlanQueues().get(16).getRoutingKey());
    }


    @Profile("background")
    @Bean
    public Binding declarePlanQueueBinding18(@Qualifier("rabbitExchangePlanProcessing") final DirectExchange exchange,
                                             PlanQueueConfiguration queueConfig) {
        return BindingBuilder.bind(queueConfig.getPlanQueues().get(17).getQueue()).to(exchange)
                .with(queueConfig.getPlanQueues().get(17).getRoutingKey());
    }


    @Profile("background")
    @Bean
    public Binding declarePlanQueueBinding19(@Qualifier("rabbitExchangePlanProcessing") final DirectExchange exchange,
                                             PlanQueueConfiguration queueConfig) {
        return BindingBuilder.bind(queueConfig.getPlanQueues().get(18).getQueue()).to(exchange)
                .with(queueConfig.getPlanQueues().get(18).getRoutingKey());
    }


    @Profile("background")
    @Bean
    public Binding declarePlanQueueBinding20(@Qualifier("rabbitExchangePlanProcessing") final DirectExchange exchange,
                                             PlanQueueConfiguration queueConfig) {
        return BindingBuilder.bind(queueConfig.getPlanQueues().get(19).getQueue()).to(exchange)
                .with(queueConfig.getPlanQueues().get(19).getRoutingKey());
    }

    @Profile("background")
    @Bean
    public Binding declareIncrementalPlanQueueBinding1(@Qualifier("rabbitExchangeIncrementalPlanProcessing") final DirectExchange exchange,
                                             PlanQueueConfiguration queueConfig) {
        return BindingBuilder.bind(queueConfig.getPlanQueuesForIncremental().get(0).getQueue()).to(exchange)
                .with(queueConfig.getPlanQueuesForIncremental().get(0).getRoutingKey());
    }

    @Profile("background")
    @Bean
    public Binding declareIncrementalPlanQueueBinding2(@Qualifier("rabbitExchangeIncrementalPlanProcessing") final DirectExchange exchange,
                                             PlanQueueConfiguration queueConfig) {
        return BindingBuilder.bind(queueConfig.getPlanQueuesForIncremental().get(1).getQueue()).to(exchange)
                .with(queueConfig.getPlanQueuesForIncremental().get(1).getRoutingKey());
    }

    @Profile("background")
    @Bean
    public Binding declareIncrementalPlanQueueBinding3(@Qualifier("rabbitExchangeIncrementalPlanProcessing") final DirectExchange exchange,
                                             PlanQueueConfiguration queueConfig) {
        return BindingBuilder.bind(queueConfig.getPlanQueuesForIncremental().get(2).getQueue()).to(exchange)
                .with(queueConfig.getPlanQueuesForIncremental().get(2).getRoutingKey());
    }

    @Profile("background")
    @Bean
    public Binding declareIncrementalPlanQueueBinding4(@Qualifier("rabbitExchangeIncrementalPlanProcessing") final DirectExchange exchange,
                                             PlanQueueConfiguration queueConfig) {
        return BindingBuilder.bind(queueConfig.getPlanQueuesForIncremental().get(3).getQueue()).to(exchange)
                .with(queueConfig.getPlanQueuesForIncremental().get(3).getRoutingKey());
    }

    @Profile("background")
    @Bean
    public Binding declareIncrementalPlanQueueBinding5(@Qualifier("rabbitExchangeIncrementalPlanProcessing") final DirectExchange exchange,
                                             PlanQueueConfiguration queueConfig) {
        return BindingBuilder.bind(queueConfig.getPlanQueuesForIncremental().get(4).getQueue()).to(exchange)
                .with(queueConfig.getPlanQueuesForIncremental().get(4).getRoutingKey());
    }

    //	@Profile("background")
//	@Bean
//	public List<Binding> declarePlanBinding(PlanQueueConfiguration queueConfig,
//			@Qualifier("rabbitExchangePlanProcessing") final DirectExchange exchange) {
//		// Binding queue with exchange
//		return queueConfig.getPlanQueues().stream().map(
//				planQueue -> BindingBuilder.bind(planQueue.getQueue()).to(exchange).with(planQueue.getRoutingKey()))
//				.collect(Collectors.toList());
//	}

    @Profile("background")
    @Bean
    @Primary
    MessageListenerAdapter planListenerAdapter(@Qualifier("planMessageConsumer") PlanMessageConsumer receiver) {
        return new MessageListenerAdapter(receiver, "receiveMessage");
    }

    @Profile("background")
    @Bean
    @Qualifier("incrementalPlanListenerAdapter")
    MessageListenerAdapter incrementalPlanListenerAdapter(@Qualifier("planMessageConsumer") PlanMessageConsumer receiver) {
        return new MessageListenerAdapter(receiver, "receiveMessage");
    }

    @Profile("background")
    @Bean
    public SimpleMessageListenerContainer planEventListenerContainer1(ConnectionFactory connectionFactory,
                                                                      @Qualifier("planListenerAdapter") MessageListenerAdapter planListenerAdapter,
                                                                      PlanQueueConfiguration queueConfig) {
        // Binding queue-names to the listener
        return createPlanEventListener(connectionFactory, planListenerAdapter, queueConfig, 0);
    }

    @Profile("background")
    @Bean
    public SimpleMessageListenerContainer planEventListenerContainer2(ConnectionFactory connectionFactory,
                                                                      @Qualifier("planListenerAdapter") MessageListenerAdapter planListenerAdapter,
                                                                      PlanQueueConfiguration queueConfig) {
        // Binding queue-names to the listener
        return createPlanEventListener(connectionFactory, planListenerAdapter, queueConfig, 1);
    }

    @Profile("background")
    @Bean
    public SimpleMessageListenerContainer planEventListenerContainer3(ConnectionFactory connectionFactory,
                                                                      @Qualifier("planListenerAdapter") MessageListenerAdapter planListenerAdapter,
                                                                      PlanQueueConfiguration queueConfig) {
        // Binding queue-names to the listener
        return createPlanEventListener(connectionFactory, planListenerAdapter, queueConfig, 2);
    }

    @Profile("background")
    @Bean
    public SimpleMessageListenerContainer planEventListenerContainer4(ConnectionFactory connectionFactory,
                                                                      @Qualifier("planListenerAdapter") MessageListenerAdapter planListenerAdapter,
                                                                      PlanQueueConfiguration queueConfig) {
        // Binding queue-names to the listener
        return createPlanEventListener(connectionFactory, planListenerAdapter, queueConfig, 3);
    }

    @Profile("background")
    @Bean
    public SimpleMessageListenerContainer planEventListenerContainer5(ConnectionFactory connectionFactory,
                                                                      @Qualifier("planListenerAdapter") MessageListenerAdapter planListenerAdapter,
                                                                      PlanQueueConfiguration queueConfig) {
        // Binding queue-names to the listener
        return createPlanEventListener(connectionFactory, planListenerAdapter, queueConfig, 4);
    }

    @Profile("background")
    @Bean
    public SimpleMessageListenerContainer planEventListenerContainer6(ConnectionFactory connectionFactory,
                                                                      @Qualifier("planListenerAdapter") MessageListenerAdapter planListenerAdapter,
                                                                      PlanQueueConfiguration queueConfig) {
        // Binding queue-names to the listener
        return createPlanEventListener(connectionFactory, planListenerAdapter, queueConfig, 5);
    }

    @Profile("background")
    @Bean
    public SimpleMessageListenerContainer planEventListenerContainer7(ConnectionFactory connectionFactory,
                                                                      @Qualifier("planListenerAdapter") MessageListenerAdapter planListenerAdapter,
                                                                      PlanQueueConfiguration queueConfig) {
        // Binding queue-names to the listener
        return createPlanEventListener(connectionFactory, planListenerAdapter, queueConfig, 6);
    }

    @Profile("background")
    @Bean
    public SimpleMessageListenerContainer planEventListenerContainer8(ConnectionFactory connectionFactory,
                                                                      @Qualifier("planListenerAdapter") MessageListenerAdapter planListenerAdapter,
                                                                      PlanQueueConfiguration queueConfig) {
        // Binding queue-names to the listener
        return createPlanEventListener(connectionFactory, planListenerAdapter, queueConfig, 7);
    }

    @Profile("background")
    @Bean
    public SimpleMessageListenerContainer planEventListenerContainer9(ConnectionFactory connectionFactory,
                                                                      @Qualifier("planListenerAdapter") MessageListenerAdapter planListenerAdapter,
                                                                      PlanQueueConfiguration queueConfig) {
        // Binding queue-names to the listener
        return createPlanEventListener(connectionFactory, planListenerAdapter, queueConfig, 8);
    }

    @Profile("background")
    @Bean
    public SimpleMessageListenerContainer planEventListenerContainer10(ConnectionFactory connectionFactory,
                                                                       @Qualifier("planListenerAdapter") MessageListenerAdapter planListenerAdapter,
                                                                       PlanQueueConfiguration queueConfig) {
        // Binding queue-names to the listener
        return createPlanEventListener(connectionFactory, planListenerAdapter, queueConfig, 9);
    }

    @Profile("background")
    @Bean
    public SimpleMessageListenerContainer planEventListenerContainer11(ConnectionFactory connectionFactory,
                                                                       @Qualifier("planListenerAdapter") MessageListenerAdapter planListenerAdapter,
                                                                       PlanQueueConfiguration queueConfig) {
        // Binding queue-names to the listener
        return createPlanEventListener(connectionFactory, planListenerAdapter, queueConfig, 10);
    }

    @Profile("background")
    @Bean
    public SimpleMessageListenerContainer planEventListenerContainer12(ConnectionFactory connectionFactory,
                                                                       @Qualifier("planListenerAdapter") MessageListenerAdapter planListenerAdapter,
                                                                       PlanQueueConfiguration queueConfig) {
        // Binding queue-names to the listener
        return createPlanEventListener(connectionFactory, planListenerAdapter, queueConfig, 11);
    }

    @Profile("background")
    @Bean
    public SimpleMessageListenerContainer planEventListenerContainer13(ConnectionFactory connectionFactory,
                                                                       @Qualifier("planListenerAdapter") MessageListenerAdapter planListenerAdapter,
                                                                       PlanQueueConfiguration queueConfig) {
        // Binding queue-names to the listener
        return createPlanEventListener(connectionFactory, planListenerAdapter, queueConfig, 12);
    }

    @Profile("background")
    @Bean
    public SimpleMessageListenerContainer planEventListenerContainer14(ConnectionFactory connectionFactory,
                                                                       @Qualifier("planListenerAdapter") MessageListenerAdapter planListenerAdapter,
                                                                       PlanQueueConfiguration queueConfig) {
        // Binding queue-names to the listener
        return createPlanEventListener(connectionFactory, planListenerAdapter, queueConfig, 13);
    }

    @Profile("background")
    @Bean
    public SimpleMessageListenerContainer planEventListenerContainer15(ConnectionFactory connectionFactory,
                                                                       @Qualifier("planListenerAdapter") MessageListenerAdapter planListenerAdapter,
                                                                       PlanQueueConfiguration queueConfig) {
        // Binding queue-names to the listener
        return createPlanEventListener(connectionFactory, planListenerAdapter, queueConfig, 14);
    }

    @Profile("background")
    @Bean
    public SimpleMessageListenerContainer planEventListenerContainer16(ConnectionFactory connectionFactory,
                                                                       @Qualifier("planListenerAdapter") MessageListenerAdapter planListenerAdapter,
                                                                       PlanQueueConfiguration queueConfig) {
        // Binding queue-names to the listener
        return createPlanEventListener(connectionFactory, planListenerAdapter, queueConfig, 15);
    }

    @Profile("background")
    @Bean
    public SimpleMessageListenerContainer planEventListenerContainer17(ConnectionFactory connectionFactory,
                                                                       @Qualifier("planListenerAdapter") MessageListenerAdapter planListenerAdapter,
                                                                       PlanQueueConfiguration queueConfig) {
        // Binding queue-names to the listener
        return createPlanEventListener(connectionFactory, planListenerAdapter, queueConfig, 16);
    }

    @Profile("background")
    @Bean
    public SimpleMessageListenerContainer planEventListenerContainer18(ConnectionFactory connectionFactory,
                                                                       @Qualifier("planListenerAdapter") MessageListenerAdapter planListenerAdapter,
                                                                       PlanQueueConfiguration queueConfig) {
        // Binding queue-names to the listener
        return createPlanEventListener(connectionFactory, planListenerAdapter, queueConfig, 17);
    }

    @Profile("background")
    @Bean
    public SimpleMessageListenerContainer planEventListenerContainer19(ConnectionFactory connectionFactory,
                                                                       @Qualifier("planListenerAdapter") MessageListenerAdapter planListenerAdapter,
                                                                       PlanQueueConfiguration queueConfig) {
        // Binding queue-names to the listener
        return createPlanEventListener(connectionFactory, planListenerAdapter, queueConfig, 18);
    }

    @Profile("background")
    @Bean
    public SimpleMessageListenerContainer planEventListenerContainer20(ConnectionFactory connectionFactory,
                                                                       @Qualifier("planListenerAdapter") MessageListenerAdapter planListenerAdapter,
                                                                       PlanQueueConfiguration queueConfig) {
        // Binding queue-names to the listener
        return createPlanEventListener(connectionFactory, planListenerAdapter, queueConfig, 19);
    }


    @Profile("background")
    @Bean
    public SimpleMessageListenerContainer incrementalPlanEventListenerContainer1(ConnectionFactory connectionFactory,
                                                                       @Qualifier("incrementalPlanListenerAdapter") MessageListenerAdapter incrementalPlanListenerAdapter,
                                                                       PlanQueueConfiguration queueConfig) {
        // Binding queue-names to the listener
        return createPlanEventListenerForIncremental(connectionFactory, incrementalPlanListenerAdapter, queueConfig, 0);
    }

    @Profile("background")
    @Bean
    public SimpleMessageListenerContainer incrementalPlanEventListenerContainer2(ConnectionFactory connectionFactory,
                                                                       @Qualifier("incrementalPlanListenerAdapter") MessageListenerAdapter incrementalPlanListenerAdapter,
                                                                       PlanQueueConfiguration queueConfig) {
        // Binding queue-names to the listener
        return createPlanEventListenerForIncremental(connectionFactory, incrementalPlanListenerAdapter, queueConfig, 1);
    }

    @Profile("background")
    @Bean
    public SimpleMessageListenerContainer incrementalPlanEventListenerContainer3(ConnectionFactory connectionFactory,
                                                                       @Qualifier("incrementalPlanListenerAdapter") MessageListenerAdapter incrementalPlanListenerAdapter,
                                                                       PlanQueueConfiguration queueConfig) {
        // Binding queue-names to the listener
        return createPlanEventListenerForIncremental(connectionFactory, incrementalPlanListenerAdapter, queueConfig, 2);
    }

    @Profile("background")
    @Bean
    public SimpleMessageListenerContainer incrementalPlanEventListenerContainer4(ConnectionFactory connectionFactory,
                                                                       @Qualifier("incrementalPlanListenerAdapter") MessageListenerAdapter incrementalPlanListenerAdapter,
                                                                       PlanQueueConfiguration queueConfig) {
        // Binding queue-names to the listener
        return createPlanEventListenerForIncremental(connectionFactory, incrementalPlanListenerAdapter, queueConfig, 3);
    }

    @Profile("background")
    @Bean
    public SimpleMessageListenerContainer incrementalPlanEventListenerContainer5(ConnectionFactory connectionFactory,
                                                                       @Qualifier("incrementalPlanListenerAdapter") MessageListenerAdapter incrementalPlanListenerAdapter,
                                                                       PlanQueueConfiguration queueConfig) {
        // Binding queue-names to the listener
        return createPlanEventListenerForIncremental(connectionFactory, incrementalPlanListenerAdapter, queueConfig, 4);
    }

    private SimpleMessageListenerContainer createPlanEventListener(ConnectionFactory connectionFactory,
                                                                   MessageListenerAdapter planListenerAdapter, PlanQueueConfiguration queueConfig, int number) {
        SimpleMessageListenerContainer container = new SimpleMessageListenerContainer();
        container.setConnectionFactory(connectionFactory);
        container.setQueueNames(queueConfig.getPlanQueues().get(number).getQueue().getName());
        container.setMessageListener(planListenerAdapter);
        container.setConcurrentConsumers(perQueueThreadPoolSize);
        container.setMaxConcurrentConsumers(perQueueThreadPoolSize);
        container.setAutoStartup(true);
        return container;
    }

    private SimpleMessageListenerContainer createPlanEventListenerForIncremental(ConnectionFactory connectionFactory,
                                                                                 @Qualifier("") MessageListenerAdapter planListenerAdapter, PlanQueueConfiguration queueConfig, int number) {
        SimpleMessageListenerContainer container = new SimpleMessageListenerContainer();
        container.setConnectionFactory(connectionFactory);
        container.setQueueNames(queueConfig.getPlanQueuesForIncremental().get(number).getQueue().getName());
        container.setMessageListener(planListenerAdapter);
        container.setConcurrentConsumers(perQueueThreadPoolSize);
        container.setMaxConcurrentConsumers(perQueueThreadPoolSize);
        container.setAutoStartup(true);
        return container;
    }

    @Profile("background")
    @Bean
    public SimpleMessageListenerContainer lpmConfigUpdateListenerContainer(ConnectionFactory connectionFactory,
                                                                           LPMUpdateListener messageListener) {
        SimpleMessageListenerContainer container = new SimpleMessageListenerContainer();
        container.setConnectionFactory(connectionFactory);
        container.setQueueNames(rabbitLPMQueue);
        container.setConcurrentConsumers(1);
        container.setMaxConcurrentConsumers(1);
        container.setMessageListener(messageListener);
        container.setAutoStartup(true);
        return container;
    }

    @Profile("docker")
    @Bean
    public SimpleMessageListenerContainer currencyUpdateListenerContainer(ConnectionFactory connectionFactory,
                                                                          CurrencyListener messageListener) {
        SimpleMessageListenerContainer container = new SimpleMessageListenerContainer();
        container.setConnectionFactory(connectionFactory);
        container.setQueueNames(rabbitCurrencyQueue);
        container.setConcurrentConsumers(1);
        container.setMaxConcurrentConsumers(1);
        container.setMessageListener(messageListener);
        container.setAutoStartup(true);
        return container;
    }

    @Profile("background")
    @Bean
    public SimpleMessageListenerContainer configUpdateListenerContainer(ConnectionFactory connectionFactory,
                                                                        ETLConfigListener messageListener) {
        SimpleMessageListenerContainer container = new SimpleMessageListenerContainer();
        container.setConnectionFactory(connectionFactory);
        container.setQueueNames(rabbitQueue);
        container.setConcurrentConsumers(1);
        container.setMaxConcurrentConsumers(1);
        container.setMessageListener(messageListener);
        container.setAutoStartup(true);
        return container;
    }

    @Bean
    public Queue getLPMMediaPlanUpdateRabbitQueue() {
        return new Queue(rabbitLPMMediaPlanUpdateQueue, true, false, false);
    }

    @Bean
    public Binding declareLPMMediaPlanBindingApp(@Qualifier("getLPMMediaPlanUpdateRabbitQueue") final Queue lpmqueue,
                                                 @Qualifier("getRabbitExchangeLPM") final DirectExchange lpmExchange) {
        Binding binding = BindingBuilder.bind(lpmqueue).to(lpmExchange).with(rabbitLPMMediaPlanUpdateRoutingKey);
        return binding;
    }

    @Profile("background")
    @Bean
    public SimpleMessageListenerContainer lpmMediaPlanUpdateListenerContainer(ConnectionFactory connectionFactory,
                                                                              LPMPlanUpdateListener messageListener) {
        SimpleMessageListenerContainer container = new SimpleMessageListenerContainer();
        container.setConnectionFactory(connectionFactory);
        container.setQueueNames(rabbitLPMMediaPlanUpdateQueue);
        container.setConcurrentConsumers(lpmPlanProcessListenerScaling);
        container.setMaxConcurrentConsumers(lpmPlanProcessListenerScaling);
        container.setMessageListener(messageListener);
        container.setAutoStartup(true);
        return container;
    }

    @Bean
    public Queue getLPMFieldValueUpdateRabbitQueue() {
        return new Queue(rabbitLPMFieldValueUpdateQueue, true, false, false);
    }

    @Bean
    public Binding declareLPMFieldValueBindingApp(@Qualifier("getLPMFieldValueUpdateRabbitQueue") final Queue lpmqueue,
                                                  @Qualifier("getRabbitExchangeLPM") final DirectExchange lpmExchange) {
        Binding binding = BindingBuilder.bind(lpmqueue).to(lpmExchange).with(rabbitLPMFieldValueUpdateRoutingKey);
        return binding;
    }

    @Profile("background")
    @Bean
    public SimpleMessageListenerContainer lpmFieldValueUpdateListenerContainer(ConnectionFactory connectionFactory,
                                                                               LPMFieldValueUpdateListener messageListener) {
        SimpleMessageListenerContainer container = new SimpleMessageListenerContainer();
        container.setConnectionFactory(connectionFactory);
        container.setQueueNames(rabbitLPMFieldValueUpdateQueue);
        container.setConcurrentConsumers(1);
        container.setMaxConcurrentConsumers(1);
        container.setMessageListener(messageListener);
        container.setAutoStartup(true);
        return container;
    }

    @Bean
    public DirectExchange getMMPObjectUpdateExchange() {
        return new DirectExchange(rabbiMMPObjectUpdateExchange);
    }

    @Bean
    public Queue getMMPFieldValueUpdateRabbitQueue() {
        return new Queue(rabbiMMPObjectUpdateMQueue, true, false, false);
    }

    @Bean
    public Binding declareMMPFieldValueBindingApp(@Qualifier("getMMPFieldValueUpdateRabbitQueue") final Queue mmpQueue,
                                                  @Qualifier("getMMPObjectUpdateExchange") final DirectExchange mmpExchange) {
        Binding binding = BindingBuilder.bind(mmpQueue).to(mmpExchange).with(rabbiMMPObjectUpdateRoutingKey);
        return binding;
    }

    @Profile("background")
    @Bean
    public SimpleMessageListenerContainer mmpFieldValueUpdateListenerContainer(ConnectionFactory connectionFactory,
                                                                               MMPObjectUpdateListener messageListener) {
        SimpleMessageListenerContainer container = new SimpleMessageListenerContainer();
        container.setConnectionFactory(connectionFactory);
        container.setQueueNames(rabbiMMPObjectUpdateMQueue);
        container.setConcurrentConsumers(lpmPlanProcessListenerScaling);
        container.setMaxConcurrentConsumers(lpmPlanProcessListenerScaling);
        container.setMessageListener(messageListener);
        container.setAutoStartup(true);
        return container;
    }
}
