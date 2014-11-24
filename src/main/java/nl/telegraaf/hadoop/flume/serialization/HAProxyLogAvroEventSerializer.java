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
            "[^ ]+ ([\\d+.]+):[^ ]+ \\[(.+?)\\] (\\S+) (\\S+)/(\\S+) ([\\d+]+)/([\\d+]+)/([\\d+]+)/([\\d+]+)/([\\d+]+) ([\\d+]*) ([\\d+]*) (\\S+) (\\S+) (\\S+) ([\\d+]+)/([\\d+]+)/([\\d+]+)/([\\d+]+)/([\\d+]+) ([\\d+]+)/([\\d+]+) \\{(.*)\\} \\{(.*)\\} \"(\\S+) (\\S+) (\\S+)\"$";


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
                    " {\"name\": \"bytesRead\", \"type\": \"string\" }," +
                    " {\"name\": \"requestCookie\", \"type\": \"string\" }," +
                    " {\"name\": \"responseCookie\", \"type\": \"string\" }," +
                    " {\"name\": \"terminationState\", \"type\": \"string\" }," +
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
                    " {\"name\": \"protocol\", \"type\": \"string\" }" +
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
            haproxyEvent.setIp(m.group(1));
            // Request time
            haproxyEvent.setTime(m.group(2));
            // Connections
            haproxyEvent.setFrontend(m.group(3));
            haproxyEvent.setBackend(m.group(4));
            haproxyEvent.setServer(m.group(5));
            // Connection timers
            haproxyEvent.setTq(Integer.valueOf(m.group(6)));
            haproxyEvent.setTw(Integer.valueOf(m.group(7)));
            haproxyEvent.setTc(Integer.valueOf(m.group(8)));
            haproxyEvent.setTr(Integer.valueOf(m.group(9)));
            haproxyEvent.setTt(Integer.valueOf(m.group(10)));
            // HTTP Status code
            haproxyEvent.setStatuscode(Integer.valueOf(m.group(11)));
            // Bytes read
            haproxyEvent.setBytesRead(m.group(12));
            // Cookies
            haproxyEvent.setRequestCookie(m.group(13));
            haproxyEvent.setResponseCookie(m.group(14));
            // Termination state
            haproxyEvent.setTerminationState(m.group(15));
            // Connection counters
            haproxyEvent.setActconn(Integer.valueOf(m.group(16)));
            haproxyEvent.setFeconn(Integer.valueOf(m.group(17)));
            haproxyEvent.setBeconn(Integer.valueOf(m.group(18)));
            haproxyEvent.setSrvconn(Integer.valueOf(m.group(19)));
            haproxyEvent.setRetries(Integer.valueOf(m.group(20)));
            // Queueus
            haproxyEvent.setSrvqueue(Integer.valueOf(m.group(21)));
            haproxyEvent.setBackendqueue(Integer.valueOf(m.group(22)));
            // Headers
            haproxyEvent.setRequestheaders(m.group(23));
            haproxyEvent.setResponseheaders(m.group(24));
            // Request
            haproxyEvent.setMethod(m.group(25));
            haproxyEvent.setUri(m.group(26));
            haproxyEvent.setProtocol(m.group(27));
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
        private String bytesRead = "";
        private String requestCookie = "";
        private String responseCookie = "";
        private String terminationState = "";
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
            builder.append(" BytesRead: ").append(bytesRead).append(", ");
            builder.append(" RequestCookie: ").append(requestCookie).append(", ");
            builder.append(" ResponseCookie: ").append(responseCookie).append(", ");
            builder.append(" TerminationState: ").append(terminationState).append(", ");
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
            builder.append(" Protocol: ").append(protocol).append("\" }");
            return builder.toString();
        }
    }
}

