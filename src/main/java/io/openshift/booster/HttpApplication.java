package io.openshift.booster;

import static io.vertx.core.http.HttpHeaders.CONTENT_TYPE;

import java.util.HashMap;
import java.util.Map;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.LoggerConfig;

import io.vertx.config.ConfigRetriever;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.StaticHandler;
import io.vertx.kafka.client.consumer.KafkaConsumer;
import io.vertx.kafka.client.producer.KafkaProducer;

/**
 *
 */
public class HttpApplication extends AbstractVerticle {

    private ConfigRetriever conf;
    private String message;

    private static final Logger LOGGER = LogManager.getLogger(HttpApplication.class);
    private JsonObject config;

    @Override
    public void start() {
        conf = ConfigRetriever.create(vertx);

        Router router = Router.router(vertx);
        router.get("/api/greeting").handler(this::greeting);
        router.get("/api/pacify").handler(this::pacify);
        router.get("/health").handler(rc -> rc.response().end("OK"));
        router.get("/").handler(StaticHandler.create());

        Map<String, String> prod_config = new HashMap<>();
        prod_config.put("bootstrap.servers", "107.23.103.79:9092");
        prod_config.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        prod_config.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        prod_config.put("acks", "1");

        // use producer for interacting with Apache Kafka
        KafkaProducer<String, String> producer = KafkaProducer.create(vertx, prod_config);
        
        Map<String, String> cons_config = new HashMap<>();
        cons_config.put("bootstrap.servers", "107.23.103.79:9092");
        cons_config.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        cons_config.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        cons_config.put("group.id", "test");
        cons_config.put("auto.offset.reset", "earliest");
        cons_config.put("enable.auto.commit", "false");

        // use consumer for interacting with Apache Kafka
        KafkaConsumer<String, String> consumer = KafkaConsumer.create(vertx, cons_config);
        
        int hash = consumer.hashCode();
        
        retrieveMessageTemplateFromConfiguration()
            .setHandler(ar -> {
                // Once retrieved, store it and start the HTTP server.
                message = ar.result();
                vertx
                    .createHttpServer()
                    .requestHandler(router::accept)
                    .listen(
                        // Retrieve the port from the configuration,
                        // default to 8080.
                        config().getInteger("http.port", 9190));

            });

        // It should use the retrieve.listen method, however it does not catch the deletion of the config map.
        // https://github.com/vert-x3/vertx-config/issues/7
        vertx.setPeriodic(2000, l -> {
            conf.getConfig(ar -> {
                if (ar.succeeded()) {
                    if (config == null || !config.encode().equals(ar.result().encode())) {
                        config = ar.result();
                        LOGGER.info("New configuration retrieved: {}",
                            ar.result().getString("message"));
                        message = ar.result().getString("message");
                        String level = ar.result().getString("level", "INFO");
                        LOGGER.info("New log level: {}", level);
                        setLogLevel(level);
                    }
                } else {
                    message = null;
                }
            });
        });
    }

    private void setLogLevel(String level) {
        LoggerContext ctx = (LoggerContext) LogManager.getContext(false);
        Configuration config = ctx.getConfiguration();
        LoggerConfig loggerConfig = config.getLoggerConfig(LogManager.ROOT_LOGGER_NAME);
        loggerConfig.setLevel(Level.getLevel(level));
        ctx.updateLoggers();
    }
    
    private void pacify(RoutingContext rc) {
    	String arg = rc.request().getParam("arg");
        if (arg == null) {
            arg = "arg";
        }

        LOGGER.debug("Replying to request, parameter={}", arg);
        JsonObject response = new JsonObject()
            .put("content", String.format(message, arg));

        rc.response()
            .putHeader(CONTENT_TYPE, "application/json; charset=utf-8")
            .end(response.encodePrettily());
    }
    
    private void greeting(RoutingContext rc) {
        if (message == null) {
            rc.response().setStatusCode(500)
                .putHeader(CONTENT_TYPE, "application/json; charset=utf-8")
                .end(new JsonObject().put("content", "no config map").encode());
            return;
        }
        String name = rc.request().getParam("name");
        if (name == null) {
            name = "World";
        }

        LOGGER.debug("Replying to request, parameter={}", name);
        JsonObject response = new JsonObject()
            .put("content", String.format(message, name));

        rc.response()
            .putHeader(CONTENT_TYPE, "application/json; charset=utf-8")
            .end(response.encodePrettily());
    }

    private Future<String> retrieveMessageTemplateFromConfiguration() {
        Future<String> future = Future.future();
        conf.getConfig(ar ->
            future.handle(ar
                .map(json -> json.getString("message"))
                .otherwise(t -> null)));
        return future;
    }
}