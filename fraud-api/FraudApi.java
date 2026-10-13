import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import java.io.*;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.regex.*;

public class FraudApi {
  public static void main(String[] args) throws Exception {
    int port = args.length > 0 ? Integer.parseInt(args[0]) : 8099;
    HttpServer server = HttpServer.create(new InetSocketAddress("0.0.0.0", port), 0);
    server.createContext("/health", e -> respond(e, 200, "{\"status\":\"UP\"}"));
    server.createContext("/api/fraud/check", e -> {
      if (!"POST".equals(e.getRequestMethod())) { respond(e,405,"{\"error\":\"POST required\"}"); return; }
      try {
        String request = read(e.getRequestBody());
        Matcher m = Pattern.compile("\\\"amount\\\"\\s*:\\s*(-?\\d+(?:\\.\\d+)?)").matcher(request);
        if (!m.find()) { respond(e,400,"{\"error\":\"numeric amount required\"}"); return; }
        java.math.BigDecimal amount = new java.math.BigDecimal(m.group(1));
        boolean rejected = amount.compareTo(new java.math.BigDecimal("10000")) > 0;
        String result = rejected ? "{\"status\":\"REJECTED\",\"reason\":\"Amount exceeds demo limit\"}" : "{\"status\":\"APPROVED\",\"reason\":\"Passed demo rule\"}";
        respond(e,200,result);
      } catch(Exception ex) { respond(e,500,"{\"error\":\"processing failed\"}"); }
    });
    server.setExecutor(java.util.concurrent.Executors.newFixedThreadPool(4));
    server.start();
    System.out.println("Fraud demo API listening on " + port);
  }
  static String read(InputStream in) throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream(); byte[] buf = new byte[4096]; int n;
    while((n=in.read(buf))!=-1) { out.write(buf,0,n); if(out.size()>65536) throw new IOException("request too large"); }
    return new String(out.toByteArray(),StandardCharsets.UTF_8);
  }
  static void respond(HttpExchange e,int status,String body) throws IOException {
    byte[] bytes=body.getBytes(StandardCharsets.UTF_8);
    e.getResponseHeaders().set("Content-Type","application/json; charset=utf-8");
    e.sendResponseHeaders(status,bytes.length);
    try(OutputStream out=e.getResponseBody()){out.write(bytes);}
  }
}
