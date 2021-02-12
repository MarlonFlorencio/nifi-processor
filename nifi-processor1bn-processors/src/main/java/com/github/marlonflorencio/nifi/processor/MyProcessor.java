package com.github.marlonflorencio.nifi.processor;

import com.github.marlonflorencio.nifi.model.Entrega;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.io.DatumReader;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.commons.io.IOUtils;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

import java.io.ByteArrayInputStream;
import java.util.*;

@Tags({"example"})
@CapabilityDescription("Provide a description")
@SeeAlso({})
@ReadsAttributes({@ReadsAttribute(attribute="", description="")})
@WritesAttributes({@WritesAttribute(attribute="", description="")})
public class MyProcessor extends AbstractProcessor {

    public static final PropertyDescriptor MY_PROPERTY = new PropertyDescriptor
            .Builder().name("MY_PROPERTY")
            .displayName("My property")
            .description("Example Property")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("Successfully converted")
            .build();

    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("Failed")
            .build();

    private List<PropertyDescriptor> descriptors;

    private Set<Relationship> relationships;

    @Override
    protected void init(final ProcessorInitializationContext context) {

        final List<PropertyDescriptor> descriptors = new ArrayList<>();
        descriptors.add(MY_PROPERTY);
        this.descriptors = Collections.unmodifiableList(descriptors);

        final Set<Relationship> relationships = new HashSet<>();
        relationships.add(REL_SUCCESS);
        relationships.add(REL_FAILURE);
        this.relationships = Collections.unmodifiableSet(relationships);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return this.relationships;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {

    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {

        ComponentLog logger = getLogger();

        logger.info(" ---->> INICIO PROCESSOR 2");

        FlowFile flowFile = session.get();
        if ( flowFile == null ) {
            return;
        }

        //final AtomicReference<Entrega> value = new AtomicReference<>();

        session.read(flowFile, in -> {
            try {

                byte[] bytes = IOUtils.toByteArray(in);

                byte[] bytes2 = Arrays.copyOfRange(bytes, 1, bytes.length);

                DatumReader<Entrega> datumReader = new SpecificDatumReader<>(Entrega.class);

                DataFileStream<Entrega>  dataFileReader = new DataFileStream<>(new ByteArrayInputStream(bytes2) , datumReader);

                while (dataFileReader.hasNext()) {
                    Entrega entrega = dataFileReader.next();
                    logger.info(" ---->> stream = " + entrega.toString());
                }
            } catch (Exception ex) {
                getLogger().error("Failed to read json string.", ex);
            }
        });

//        // Write the results to an attribute
//        String results = value.get();
//        if (results != null && !results.isEmpty()) {
//            flowFile = session.putAttribute(flowFile, "match", results);
//        }
//
//        // To write the results back out ot flow file
//        flowFile = session.write(flowFile, out -> out.write(value.get().getBytes()));

        logger.info(" ---->> FIM PROCESSOR");

        session.transfer(flowFile, REL_SUCCESS);
    }

}
