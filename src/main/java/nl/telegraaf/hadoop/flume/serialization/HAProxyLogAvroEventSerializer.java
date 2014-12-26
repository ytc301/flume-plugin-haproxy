package nl.telegraaf.hadoop.flume.serialization;

import com.google.common.base.Charsets;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.java.Log;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.serialization.AbstractAvroEventSerializer;
import org.apache.flume.serialization.EventSerializer;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import nl.telegraaf.hadoop.flume.serialization.HAProxyLogAvroEventSerializer.HAProxyEvent;

/**
 */
@Slf4j
public class HAProxyLogAvroEventSerializer extends AbstractAvroEventSerializer<HAProxyEvent> {

    // Log format:
    // haproxy[18439]: 70.208.71.125:1545 [24/Nov/2014:13:59:25.582] http nginx/blink182 17/0/1/4/24 200 44332 cc=clienttime-1378732583324.version-35.essential.f - ---- 2422/2288/2/1/0 0/0 {www.telegraaf.nl|Mozilla/5.0 (iPad; CPU OS 7_1_1 like Mac OS X) AppleWebKit/537.51.2 (KHTML, like} {text/html} "GET /telesport/ HTTP/1.1"

    private static final String REGEXP =
            "[^ ]+ (?<clientip>[\\d+.]+):[^ ]+ \\[(?<acceptdate>.+?)\\] (?<frontendname>\\S+) (?<backendname>\\S+)/(?<servername>\\S+) (?<timerequest>[\\d+]+)/(?<timequeue>[\\d+]+)/(?<timebackendconnect>[\\d+]+)/(?<timebackendresponse>[\\d+]+)/(?<timeduration>[\\d+]+) (?<httpstatuscode>[\\d+]*) (?<bytesread>[\\d+]*) (?<capturedrequestcookie>\\S+) (?<capturedresponsecookie>\\S+) (?<terminationstate>\\S+) (?<actconn>[\\d+]+)/(?<feconn>[\\d+]+)/(?<beconn>[\\d+]+)/(?<srvconn>[\\d+]+)/(?<retries>[\\d+]+) (?<serverqueue>[\\d+]+)/(?<backendqueue>[\\d+]+) \\{(?<capturedrequestheaders>.*)\\} \\{(?<capturedreponseheaders>.*)\\} \"(?<httpmethod>\\S+) (?<httprequest>\\S+)(\\s+(?<httpversion>\\S+)\")?";


    private static final Schema SCHEMA = new Schema.Parser().parse(
            "{ \"type\":\"record\", \"name\": \"HAProxyEvent\", \"namespace\": \"nl.telegraaf.flume\", \"fields\": [" +
                    " {\"name\": \"headers\", \"type\": { \"type\": \"map\", \"values\": \"string\" } }, " +
                    " {\"name\": \"original\", \"type\": \"string\" }," +
                    " {\"name\": \"ip\", \"type\": \"string\" }," +
                    " {\"name\": \"time\", \"type\": \"string\" }," +
                    " {\"name\": \"frontend\", \"type\": \"string\" }," +
                    " {\"name\": \"backend\", \"type\": \"string\" }," +
                    " {\"name\": \"server\", \"type\": \"string\" }," +
                    " {\"name\": \"tq\", \"type\": \"int\" }," +
                    " {\"name\": \"tw\", \"type\": \"int\" }," +
                    " {\"name\": \"tc\", \"type\": \"int\" }," +
                    " {\"name\": \"tr\", \"type\": \"int\" }," +
                    " {\"name\": \"tt\", \"type\": \"int\" }," +
                    " {\"name\": \"statuscode\", \"type\": \"int\" }," +
                    " {\"name\": \"bytesread\", \"type\": \"string\" }," +
                    " {\"name\": \"requestcookie\", \"type\": \"string\" }," +
                    " {\"name\": \"responsecookie\", \"type\": \"string\" }," +
                    " {\"name\": \"terminationstate\", \"type\": \"string\" }," +
                    " {\"name\": \"actconn\", \"type\": \"int\" }," +
                    " {\"name\": \"feconn\", \"type\": \"int\" }," +
                    " {\"name\": \"beconn\", \"type\": \"int\" }," +
                    " {\"name\": \"srvconn\", \"type\": \"int\" }," +
                    " {\"name\": \"retries\", \"type\": \"int\" }," +
                    " {\"name\": \"srvqueue\", \"type\": \"int\" }," +
                    " {\"name\": \"backendqueue\", \"type\": \"int\" }," +
                    " {\"name\": \"requestheaders\", \"type\": \"string\" }," +
                    " {\"name\": \"responseheaders\", \"type\": \"string\" }," +
                    " {\"name\": \"method\", \"type\": \"string\" }," +
                    " {\"name\": \"uri\", \"type\": \"string\" }," +
                    " {\"name\": \"protocol\", \"type\": [\"null\", \"string\"] }" +
                    "] }");

    private final OutputStream out;

    public HAProxyLogAvroEventSerializer(OutputStream out) throws IOException {
        this.out = out;
    }

    @Override
    protected OutputStream getOutputStream() {
        return out;
    }

    @Override
    protected Schema getSchema() {
        return SCHEMA;
    }

    @Override
    protected HAProxyEvent convert(Event event) {
        HAProxyEvent haproxyEvent = new HAProxyEvent();

        String logline = new String(event.getBody(), Charsets.UTF_8);
        haproxyEvent.setHeaders(event.getHeaders());
        haproxyEvent.setOriginal(logline);

        Pattern pattern = Pattern.compile(REGEXP);
        Matcher m = pattern.matcher(logline);
        if (m.matches()){
            // Client IP
            haproxyEvent.setIp(m.group("clientip"));
            // Request time
            haproxyEvent.setTime(m.group("acceptdate"));
            // Connections
            haproxyEvent.setFrontend(m.group("frontendname"));
            haproxyEvent.setBackend(m.group("backendname"));
            haproxyEvent.setServer(m.group("servername"));
            // Connection timers
            haproxyEvent.setTq(Integer.valueOf(m.group("timerequest")));
            haproxyEvent.setTw(Integer.valueOf(m.group("timequeue")));
            haproxyEvent.setTc(Integer.valueOf(m.group("timebackendconnect")));
            haproxyEvent.setTr(Integer.valueOf(m.group("timebackendresponse")));
            haproxyEvent.setTt(Integer.valueOf(m.group("timeduration")));
            // HTTP Status code
            haproxyEvent.setStatuscode(Integer.valueOf(m.group("httpstatuscode")));
            // Bytes read
            haproxyEvent.setBytesread(m.group("bytesread"));
            // Cookies
            haproxyEvent.setRequestcookie(m.group("capturedrequestcookie"));
            haproxyEvent.setResponsecookie(m.group("capturedresponsecookie"));
            // Termination state
            haproxyEvent.setTerminationstate(m.group("terminationstate"));
            // Connection counters
            haproxyEvent.setActconn(Integer.valueOf(m.group("actconn")));
            haproxyEvent.setFeconn(Integer.valueOf(m.group("feconn")));
            haproxyEvent.setBeconn(Integer.valueOf(m.group("beconn")));
            haproxyEvent.setSrvconn(Integer.valueOf(m.group("srvconn")));
            haproxyEvent.setRetries(Integer.valueOf(m.group("retries")));
            // Queueus
            haproxyEvent.setSrvqueue(Integer.valueOf(m.group("serverqueue")));
            haproxyEvent.setBackendqueue(Integer.valueOf(m.group("backendqueue")));
            // Headers
            haproxyEvent.setRequestheaders(m.group("capturedrequestheaders"));
            haproxyEvent.setResponseheaders(m.group("capturedreponseheaders"));
            // Request
            haproxyEvent.setMethod(m.group("httpmethod"));
            haproxyEvent.setUri(m.group("httprequest"));
            haproxyEvent.setProtocol(m.group("httpversion"));
        } else {
            log.warn("The event doesn't match the HAProxy LogFormat! [{}]", logline);
        }

        //log.debug("Serialized event as: {}", haproxyEvent);

        return haproxyEvent;
    }

    public static class Builder implements EventSerializer.Builder {

        @Override
        public EventSerializer build(Context context, OutputStream out) {
            HAProxyLogAvroEventSerializer writer = null;
            try {
                writer = new HAProxyLogAvroEventSerializer(out);
                writer.configure(context);
            } catch (IOException e) {
                log.error("Unable to parse schema file. Exception follows.", e);
            }
            return writer;
        }

    }

    // This class would ideally be generated from the avro schema file,
    // but we are letting reflection do the work instead.
    // There's no great reason not to let Avro generate it.
    @Setter
    @Getter
    public static class HAProxyEvent {
        private Map<String, String> headers;
        private String original = "";
        private String ip = "";
        private String time = "";
        private String frontend = "";
        private String backend = "";
        private String server = "";
        private int tq;
        private int tw;
        private int tc;
        private int tr;
        private int tt;
        private int statuscode;
        private String bytesread = "";
        private String requestcookie = "";
        private String responsecookie = "";
        private String terminationstate = "";
        private int actconn;
        private int feconn;
        private int beconn;
        private int srvconn;
        private int retries;
        private int srvqueue;
        private int backendqueue;
        private String requestheaders = "";
        private String responseheaders = "";
        private String method = "";
        private String uri = "";
        private String protocol = "";

        @Override
        public String toString() {
            StringBuilder builder = new StringBuilder();
            builder.append("{ Original: ").append(original).append(", ");
            builder.append(" IP: ").append(ip).append(", ");
            builder.append(" Time: ").append(time).append(", ");
            builder.append(" Frontend: ").append(frontend).append(", ");
            builder.append(" Backend: ").append(backend).append(", ");
            builder.append(" Server: ").append(server).append(", ");
            builder.append(" TimingsTq: ").append(tq).append(", ");
            builder.append(" TimingsTw: ").append(tw).append(", ");
            builder.append(" TimingsTc: ").append(tc).append(", ");
            builder.append(" TimingsTr: ").append(tr).append(", ");
            builder.append(" TimingsTt: ").append(tt).append(", ");
            builder.append(" Statuscode: ").append(statuscode).append(", ");
            builder.append(" BytesRead: ").append(bytesread).append(", ");
            builder.append(" RequestCookie: ").append(requestcookie).append(", ");
            builder.append(" ResponseCookie: ").append(responsecookie).append(", ");
            builder.append(" TerminationState: ").append(terminationstate).append(", ");
            builder.append(" Actconn: ").append(actconn).append(", ");
            builder.append(" Feconn: ").append(feconn).append(", ");
            builder.append(" Beconn: ").append(beconn).append(", ");
            builder.append(" Srvconn: ").append(srvconn).append(", ");
            builder.append(" Retries: ").append(retries).append(", ");
            builder.append(" Srvqueue: ").append(srvqueue).append(", ");
            builder.append(" Backendqueue: ").append(backendqueue).append(", ");
            builder.append(" Requestheaders: ").append(requestheaders).append(", ");
            builder.append(" Responseheaders: ").append(responseheaders).append(", ");
            builder.append(" Method: ").append(method).append(", ");
            builder.append(" URI: ").append(uri).append(", ");
            builder.append(" Protocol: ").append(protocol).append(" }");
            // builder.append(" Protocol: ").append(protocol).append("\" }");
            return builder.toString();
        }
    }
}

