package com.github.marlonflorencio.nifi.processor;


import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.Validator;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.util.FormatUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.regex.Pattern;

public final class KafkaProcessorUtils {
    private static final String ALLOW_EXPLICIT_KEYTAB = "NIFI_ALLOW_EXPLICIT_KEYTAB";

    final Logger logger = LoggerFactory.getLogger(this.getClass());

    static final AllowableValue UTF8_ENCODING = new AllowableValue("utf-8", "UTF-8 Encoded", "The key is interpreted as a UTF-8 Encoded string.");
    static final AllowableValue HEX_ENCODING = new AllowableValue("hex", "Hex Encoded",
            "The key is interpreted as arbitrary binary data and is encoded using hexadecimal characters with uppercase letters");
    static final AllowableValue DO_NOT_ADD_KEY_AS_ATTRIBUTE = new AllowableValue("do-not-add", "Do Not Add Key as Attribute",
            "The key will not be added as an Attribute");

    static final Pattern HEX_KEY_PATTERN = Pattern.compile("(?:[0123456789abcdefABCDEF]{2})+");

    static final String KAFKA_KEY = "kafka.key";
    static final String KAFKA_TOPIC = "kafka.topic";
    static final String KAFKA_PARTITION = "kafka.partition";
    static final String KAFKA_OFFSET = "kafka.offset";
    static final String KAFKA_TIMESTAMP = "kafka.timestamp";
    static final String KAFKA_COUNT = "kafka.count";

    static final AllowableValue SEC_PLAINTEXT = new AllowableValue("PLAINTEXT", "PLAINTEXT", "PLAINTEXT");
    static final AllowableValue SEC_SSL = new AllowableValue("SSL", "SSL", "SSL");
    public static final AllowableValue SEC_SASL_PLAINTEXT = new AllowableValue("SASL_PLAINTEXT", "SASL_PLAINTEXT", "SASL_PLAINTEXT");
    public static final AllowableValue SEC_SASL_SSL = new AllowableValue("SASL_SSL", "SASL_SSL", "SASL_SSL");

    static final String GSSAPI_VALUE = "GSSAPI";
    static final AllowableValue SASL_MECHANISM_GSSAPI = new AllowableValue(GSSAPI_VALUE, GSSAPI_VALUE,
            "The mechanism for authentication via Kerberos. The principal and keytab must be provided to the processor " +
                    "by using a Keytab Credential service, or by specifying the properties directly in the processor.");

    static final String PLAIN_VALUE = "PLAIN";
    static final AllowableValue SASL_MECHANISM_PLAIN = new AllowableValue(PLAIN_VALUE, PLAIN_VALUE,
            "The mechanism for authentication via username and password. The username and password properties must " +
                    "be populated when using this mechanism.");

    static final String SCRAM_SHA256_VALUE = "SCRAM-SHA-256";
    static final AllowableValue SASL_MECHANISM_SCRAM_SHA256 = new AllowableValue(SCRAM_SHA256_VALUE, SCRAM_SHA256_VALUE,"The Salted Challenge Response Authentication Mechanism using SHA-256. " +
            "The username and password properties must be set when using this mechanism.");

    static final String SCRAM_SHA512_VALUE = "SCRAM-SHA-512";
    static final AllowableValue SASL_MECHANISM_SCRAM_SHA512 = new AllowableValue(SCRAM_SHA512_VALUE, SCRAM_SHA512_VALUE,"The Salted Challenge Response Authentication Mechanism using SHA-512. " +
            "The username and password properties must be set when using this mechanism.");

    public static final PropertyDescriptor BOOTSTRAP_SERVERS = new PropertyDescriptor.Builder()
            .name(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG)
            .displayName("Kafka Brokers")
            .description("A comma-separated list of known Kafka Brokers in the format <host>:<port>")
            .required(true)
            .addValidator(StandardValidators.HOSTNAME_PORT_LIST_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .defaultValue("localhost:9092")
            .build();
    public static final PropertyDescriptor SECURITY_PROTOCOL = new PropertyDescriptor.Builder()
            .name("security.protocol")
            .displayName("Security Protocol")
            .description("Protocol used to communicate with brokers. Corresponds to Kafka's 'security.protocol' property.")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .allowableValues(SEC_PLAINTEXT, SEC_SSL, SEC_SASL_PLAINTEXT, SEC_SASL_SSL)
            .defaultValue(SEC_PLAINTEXT.getValue())
            .build();
    static final PropertyDescriptor SASL_MECHANISM = new PropertyDescriptor.Builder()
            .name("sasl.mechanism")
            .displayName("SASL Mechanism")
            .description("The SASL mechanism to use for authentication. Corresponds to Kafka's 'sasl.mechanism' property.")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .allowableValues(SASL_MECHANISM_GSSAPI, SASL_MECHANISM_PLAIN, SASL_MECHANISM_SCRAM_SHA256, SASL_MECHANISM_SCRAM_SHA512)
            .defaultValue(GSSAPI_VALUE)
            .build();
    public static final PropertyDescriptor JAAS_SERVICE_NAME = new PropertyDescriptor.Builder()
            .name("sasl.kerberos.service.name")
            .displayName("Kerberos Service Name")
            .description("The service name that matches the primary name of the Kafka server configured in the broker JAAS file."
                    + "This can be defined either in Kafka's JAAS config or in Kafka's config. "
                    + "Corresponds to Kafka's 'security.protocol' property."
                    + "It is ignored unless one of the SASL options of the <Security Protocol> are selected.")
            .required(false)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();
    static final PropertyDescriptor USER_PRINCIPAL = new PropertyDescriptor.Builder()
            .name("sasl.kerberos.principal")
            .displayName("Kerberos Principal")
            .description("The Kerberos principal that will be used to connect to brokers. If not set, it is expected to set a JAAS configuration file "
                    + "in the JVM properties defined in the bootstrap.conf file. This principal will be set into 'sasl.jaas.config' Kafka's property.")
            .required(false)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();
    static final PropertyDescriptor USER_KEYTAB = new PropertyDescriptor.Builder()
            .name("sasl.kerberos.keytab")
            .displayName("Kerberos Keytab")
            .description("The Kerberos keytab that will be used to connect to brokers. If not set, it is expected to set a JAAS configuration file "
                    + "in the JVM properties defined in the bootstrap.conf file. This principal will be set into 'sasl.jaas.config' Kafka's property.")
            .required(false)
            .addValidator(StandardValidators.FILE_EXISTS_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();
    static final PropertyDescriptor USERNAME = new PropertyDescriptor.Builder()
            .name("sasl.username")
            .displayName("Username")
            .description("The username when the SASL Mechanism is " + PLAIN_VALUE + " or " + SCRAM_SHA256_VALUE + "/" + SCRAM_SHA512_VALUE)
            .required(false)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();
    static final PropertyDescriptor PASSWORD = new PropertyDescriptor.Builder()
            .name("sasl.password")
            .displayName("Password")
            .description("The password for the given username when the SASL Mechanism is " + PLAIN_VALUE + " or " + SCRAM_SHA256_VALUE + "/" + SCRAM_SHA512_VALUE)
            .required(false)
            .sensitive(true)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();
    static final PropertyDescriptor TOKEN_AUTH = new PropertyDescriptor.Builder()
            .name("sasl.token.auth")
            .displayName("Token Auth")
            .description("When " + SASL_MECHANISM.getDisplayName() + " is " + SCRAM_SHA256_VALUE + " or " + SCRAM_SHA512_VALUE
                    + ", this property indicates if token authentication should be used.")
            .required(false)
            .allowableValues("true", "false")
            .defaultValue("false")
            .build();


    static List<PropertyDescriptor> getCommonPropertyDescriptors() {
        return Arrays.asList(
                BOOTSTRAP_SERVERS,
                SECURITY_PROTOCOL,
                SASL_MECHANISM,
                JAAS_SERVICE_NAME,
                USER_PRINCIPAL,
                USER_KEYTAB,
                USERNAME,
                PASSWORD,
                TOKEN_AUTH
        );
    }

    public static Collection<ValidationResult> validateCommonProperties(final ValidationContext validationContext) {
        List<ValidationResult> results = new ArrayList<>();

        final String securityProtocol = validationContext.getProperty(SECURITY_PROTOCOL).getValue();
        final String saslMechanism = validationContext.getProperty(SASL_MECHANISM).getValue();

        final String explicitPrincipal = validationContext.getProperty(USER_PRINCIPAL).evaluateAttributeExpressions().getValue();
        final String explicitKeytab = validationContext.getProperty(USER_KEYTAB).evaluateAttributeExpressions().getValue();


        final String allowExplicitKeytabVariable = System.getenv(ALLOW_EXPLICIT_KEYTAB);
        if ("false".equalsIgnoreCase(allowExplicitKeytabVariable) && (explicitPrincipal != null || explicitKeytab != null)) {
            results.add(new ValidationResult.Builder()
                    .subject("Kerberos Credentials")
                    .valid(false)
                    .explanation("The '" + ALLOW_EXPLICIT_KEYTAB + "' system environment variable is configured to forbid explicitly configuring principal/keytab in processors. "
                            + "The Kerberos Credentials Service should be used instead of setting the Kerberos Keytab or Kerberos Principal property.")
                    .build());
        }

        // validates that if the SASL mechanism is GSSAPI (kerberos) AND one of the SASL options is selected
        // for security protocol, then Kerberos principal is provided as well
        if (SASL_MECHANISM_GSSAPI.getValue().equals(saslMechanism)
                && (SEC_SASL_PLAINTEXT.getValue().equals(securityProtocol) || SEC_SASL_SSL.getValue().equals(securityProtocol))) {
            String jaasServiceName = validationContext.getProperty(JAAS_SERVICE_NAME).evaluateAttributeExpressions().getValue();
            if (jaasServiceName == null || jaasServiceName.trim().length() == 0) {
                results.add(new ValidationResult.Builder().subject(JAAS_SERVICE_NAME.getDisplayName()).valid(false)
                        .explanation("The <" + JAAS_SERVICE_NAME.getDisplayName() + "> property must be set when <"
                                + SECURITY_PROTOCOL.getDisplayName() + "> is configured as '"
                                + SEC_SASL_PLAINTEXT.getValue() + "' or '" + SEC_SASL_SSL.getValue() + "'.")
                        .build());
            }
        }

        // validate that if SASL Mechanism is PLAIN or SCRAM, then username and password are both provided
        if (SASL_MECHANISM_PLAIN.getValue().equals(saslMechanism)
                || SASL_MECHANISM_SCRAM_SHA256.getValue().equals(saslMechanism)
                || SASL_MECHANISM_SCRAM_SHA512.getValue().equals(saslMechanism)) {
            final String username = validationContext.getProperty(USERNAME).evaluateAttributeExpressions().getValue();
            if (StringUtils.isBlank(username)) {
                results.add(new ValidationResult.Builder()
                        .subject(USERNAME.getDisplayName())
                        .valid(false)
                        .explanation("A username is required when " + SASL_MECHANISM.getDisplayName()
                                + " is " + PLAIN_VALUE + " or " + SCRAM_SHA256_VALUE + "/" + SCRAM_SHA512_VALUE)
                        .build());
            }

            final String password = validationContext.getProperty(PASSWORD).evaluateAttributeExpressions().getValue();
            if (StringUtils.isBlank(password)) {
                results.add(new ValidationResult.Builder()
                        .subject(PASSWORD.getDisplayName())
                        .valid(false)
                        .explanation("A password is required when " + SASL_MECHANISM.getDisplayName()
                                + " is " + PLAIN_VALUE + " or " + SCRAM_SHA256_VALUE + "/" + SCRAM_SHA512_VALUE)
                        .build());
            }
        }

        final String enableAutoCommit = validationContext.getProperty(new PropertyDescriptor.Builder().name(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG).build()).getValue();
        if (enableAutoCommit != null && !enableAutoCommit.toLowerCase().equals("false")) {
            results.add(new ValidationResult.Builder().subject(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG)
                    .explanation("Enable auto commit must be false. It is managed by the processor.").build());
        }

        final String keySerializer = validationContext.getProperty(new PropertyDescriptor.Builder().name(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG).build()).getValue();
        if (keySerializer != null && !ByteArraySerializer.class.getName().equals(keySerializer)) {
            results.add(new ValidationResult.Builder().subject(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG)
                    .explanation("Key Serializer must be " + ByteArraySerializer.class.getName() + "' was '" + keySerializer + "'").build());
        }

        final String valueSerializer = validationContext.getProperty(new PropertyDescriptor.Builder().name(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG).build()).getValue();
        if (valueSerializer != null && !ByteArraySerializer.class.getName().equals(valueSerializer)) {
            results.add(new ValidationResult.Builder().subject(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG)
                    .explanation("Value Serializer must be " + ByteArraySerializer.class.getName() + "' was '" + valueSerializer + "'").build());
        }

        final String keyDeSerializer = validationContext.getProperty(new PropertyDescriptor.Builder().name(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG).build()).getValue();
        if (keyDeSerializer != null && !ByteArrayDeserializer.class.getName().equals(keyDeSerializer)) {
            results.add(new ValidationResult.Builder().subject(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG)
                    .explanation("Key De-Serializer must be '" + ByteArrayDeserializer.class.getName() + "' was '" + keyDeSerializer + "'").build());
        }

        final String valueDeSerializer = validationContext.getProperty(new PropertyDescriptor.Builder().name(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG).build()).getValue();
        if (valueDeSerializer != null && !ByteArrayDeserializer.class.getName().equals(valueDeSerializer)) {
            results.add(new ValidationResult.Builder().subject(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG)
                    .explanation("Value De-Serializer must be " + ByteArrayDeserializer.class.getName() + "' was '" + valueDeSerializer + "'").build());
        }

        return results;
    }

    public static final class KafkaConfigValidator implements Validator {

        final Class<?> classType;

        public KafkaConfigValidator(final Class<?> classType) {
            this.classType = classType;
        }

        @Override
        public ValidationResult validate(final String subject, final String value, final ValidationContext context) {
            if (subject.startsWith(ConsumerPartitionsUtil.PARTITION_PROPERTY_NAME_PREFIX)) {
                return new ValidationResult.Builder().valid(true).build();
            }

            final boolean knownValue = KafkaProcessorUtils.isStaticStringFieldNamePresent(subject, classType, CommonClientConfigs.class, SslConfigs.class, SaslConfigs.class);
            return new ValidationResult.Builder().subject(subject).explanation("Must be a known configuration parameter for this kafka client").valid(knownValue).build();
        }
    }

    /**
     * Builds transit URI for provenance event. The transit URI will be in the
     * form of &lt;security.protocol&gt;://&lt;bootstrap.servers&gt;/topic
     */
    static String buildTransitURI(String securityProtocol, String brokers, String topic) {
        StringBuilder builder = new StringBuilder();
        builder.append(securityProtocol);
        builder.append("://");
        builder.append(brokers);
        builder.append("/");
        builder.append(topic);
        return builder.toString();
    }

    static void buildCommonKafkaProperties(final ProcessContext context, final Class<?> kafkaConfigClass, final Map<String, Object> mapToPopulate) {
        for (PropertyDescriptor propertyDescriptor : context.getProperties().keySet()) {

            String propertyName = propertyDescriptor.getName();
            String propertyValue = propertyDescriptor.isExpressionLanguageSupported()
                    ? context.getProperty(propertyDescriptor).evaluateAttributeExpressions().getValue()
                    : context.getProperty(propertyDescriptor).getValue();

            if (propertyValue != null && !propertyName.equals(USER_PRINCIPAL.getName()) && !propertyName.equals(USER_KEYTAB.getName())
                    && !propertyName.startsWith(ConsumerPartitionsUtil.PARTITION_PROPERTY_NAME_PREFIX)) {

                // If the property name ends in ".ms" then it is a time period. We want to accept either an integer as number of milliseconds
                // or the standard NiFi time period such as "5 secs"
                if (propertyName.endsWith(".ms") && !StringUtils.isNumeric(propertyValue.trim())) { // kafka standard time notation
                    propertyValue = String.valueOf(FormatUtils.getTimeDuration(propertyValue.trim(), TimeUnit.MILLISECONDS));
                }

                if (isStaticStringFieldNamePresent(propertyName, kafkaConfigClass, CommonClientConfigs.class, SslConfigs.class, SaslConfigs.class)) {
                    mapToPopulate.put(propertyName, propertyValue);
                }
            }
        }
    }

    public static boolean isStaticStringFieldNamePresent(final String name, final Class<?>... classes) {
        return KafkaProcessorUtils.getPublicStaticStringFieldValues(classes).contains(name);
    }

    private static Set<String> getPublicStaticStringFieldValues(final Class<?>... classes) {
        final Set<String> strings = new HashSet<>();
        for (final Class<?> classType : classes) {
            for (final Field field : classType.getDeclaredFields()) {
                if (Modifier.isPublic(field.getModifiers()) && Modifier.isStatic(field.getModifiers()) && field.getType().equals(String.class)) {
                    try {
                        strings.add(String.valueOf(field.get(null)));
                    } catch (IllegalArgumentException | IllegalAccessException ex) {
                        //ignore
                    }
                }
            }
        }
        return strings;
    }

}