package nl.telegraaf.hadoop.flume.serialization;

import com.google.common.base.Charsets;
import com.google.common.collect.Lists;
import lombok.extern.java.Log;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.commons.io.FileUtils;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.event.EventBuilder;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.List;

import org.apache.flume.serialization.EventSerializer;
import org.apache.flume.serialization.EventSerializerFactory;
import org.junit.Assert;
import org.junit.Test;

/**
 */
@Slf4j
public class TestHAProxyAvroEventSerializer {

    File testFile = new File("src/test/resources/HAProxyEvents.avro");

    private static List<Event> generateHAProxyEvents() {
        List<Event> list = Lists.newArrayList();

        Event e;

        // generate some events
        e = EventBuilder.withBody("haproxy[18439]: 70.208.71.125:1545 [24/Nov/2014:13:59:25.582] http nginx/blink182 17/0/1/4/24 200 44332 cc=clienttime-1378732583324.version-35.essential.f - ---- 2422/2288/2/1/0 0/0 {www.telegraaf.nl|Mozilla/5.0 (iPad; CPU OS 7_1_1 like Mac OS X) AppleWebKit/537.51.2 (KHTML, like} {text/html} \"GET /telesport/ HTTP/1.1\"", Charsets.UTF_8);

        list.add(e);

        e = EventBuilder.withBody("haproxy[13533]: 70.208.71.125:55080 [24/Nov/2014:13:59:25.609] http nginx/oasis 8/0/3/2/14 200 23067 cc=clienttime-1364315552944.version-4.essential.fu - ---- 2860/2739/9/3/0 0/0 {www.telegraaf.nl|Mozilla/5.0 (iPad; CPU OS 7_1_2 like Mac OS X) AppleWebKit/537.51.2 (KHTML, like} {text/html;charset=UTF-8} \"GET http://www.telegraaf.nl/telesport/voetbal/az/23415908/__Berghuis_keert__terug_bij_AZ__.html  HTTP/1.1\"", Charsets.UTF_8);
        list.add(e);

        e = EventBuilder.withBody("haproxy[13533]: 70.208.71.125:55080 [24/Nov/2014:13:59:25.609] http nginx/oasis 8/0/3/2/14 200 23067 cc=clienttime-1364315552944.version-4.essential.fu - ---- 2860/2739/9/3/0 0/0 {www.telegraaf.nl|Mozilla/5.0 (iPad; CPU OS 7_1_2 like Mac OS X) AppleWebKit/537.51.2 (KHTML, like} {text/html;charset=UTF-8} \"GET http://www.telegraaf.nl/telesport/voetbal/az/23415908/__Berghuis_keert__terug_bij_AZ__.h", Charsets.UTF_8);
        list.add(e);

        log.info("Event: {}", e);

        return list;
    }

    @Test
    public void test() throws FileNotFoundException, IOException {

        // create the file, write some data
        OutputStream out = new FileOutputStream(testFile);
        String builderName = HAProxyLogAvroEventSerializer.Builder.class.getName();

        Context ctx = new Context();
        ctx.put("syncInterval", "4096");

        EventSerializer serializer =
                EventSerializerFactory.getInstance(builderName, ctx, out);
        serializer.afterCreate(); // must call this when a file is newly created

        List<Event> events = generateHAProxyEvents();
        for (Event e : events) {
            serializer.write(e);
        }
        serializer.flush();
        serializer.beforeClose();
        out.flush();
        out.close();

        // now try to read the file back

        DatumReader<GenericRecord> reader = new GenericDatumReader<GenericRecord>();
        DataFileReader<GenericRecord> fileReader =
                new DataFileReader<GenericRecord>(testFile, reader);

        GenericRecord record = new GenericData.Record(fileReader.getSchema());
        int numEvents = 0;
        while (fileReader.hasNext()) {
            fileReader.next(record);
            String ip = record.get("ip").toString();
            String uri = record.get("uri").toString();
            Integer statuscode = (Integer) record.get("statuscode");
            String frontend = record.get("frontend").toString();
            String original = record.get("original").toString();
            String timingstt = record.get("tt").toString();
            String requestcookie = record.get("requestcookie").toString();
            String terminationstate = record.get("terminationstate").toString();

            Assert.assertEquals("Frontend should be 'http'", "http", frontend);
            Assert.assertEquals("Ip should be 70.208.71.125", "70.208.71.125", ip);

            System.out.println("\n\nIP " + ip + "\nrequested: " + uri + "\nstatus code: " + statuscode + "\ntimings totals: " + timingstt + "\nrequestcookie: " + requestcookie);
            System.out.println("Termination State: " + terminationstate);
            System.out.println("Original logline: " + original);
            System.out.println("Record: " + record);
            numEvents++;
        }

        fileReader.close();
        Assert.assertEquals("Should have found a total of 3 events", 3, numEvents);

        FileUtils.forceDelete(testFile);
    }

}
