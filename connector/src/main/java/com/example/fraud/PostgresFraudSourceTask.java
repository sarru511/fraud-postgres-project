package com.example.fraud;

import org.apache.kafka.connect.source.SourceTask;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.data.Schema;
import java.io.*;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.util.*;
import java.util.regex.*;

public class PostgresFraudSourceTask extends SourceTask {
  private String jdbcUrl, user, password, apiUrl;
  private int interval, batch;
  private volatile boolean running;
  private long lastId;
  private static final Map<String,String> PARTITION = Collections.singletonMap("source","ecommerce.public.fraud_payments");

  public void start(Map<String,String> props) {
    jdbcUrl = required(props,"jdbc.url"); user = required(props,"jdbc.user"); password = required(props,"jdbc.password"); apiUrl = required(props,"fraud.api.url");
    interval = Integer.parseInt(props.getOrDefault("poll.interval.ms","3000"));
    batch = Integer.parseInt(props.getOrDefault("batch.size","100"));
    if (interval < 100 || batch < 1 || batch > 10000) throw new IllegalArgumentException("Invalid interval/batch");
    Map<String,Object> previous = context.offsetStorageReader().offset(PARTITION);
    lastId = previous != null && previous.get("last_id") != null ? ((Number)previous.get("last_id")).longValue() : 0L;
    running = true;
    System.out.println("Fraud source task started; last committed ID="+lastId);
  }
  private static String required(Map<String,String> props,String key) {
    String value=props.get(key); if(value==null || value.isEmpty()) throw new IllegalArgumentException("Missing "+key); return value;
  }
  public List<SourceRecord> poll() throws InterruptedException {
    if(!running) return Collections.emptyList();
    List<SourceRecord> records = new ArrayList<>();
    try (Connection conn=DriverManager.getConnection(jdbcUrl,user,password);
         PreparedStatement stmt=conn.prepareStatement("SELECT id, name, amount FROM public.fraud_payments WHERE id > ? ORDER BY id LIMIT ?")) {
      stmt.setLong(1,lastId); stmt.setInt(2,batch);
      try(ResultSet rs=stmt.executeQuery()) {
        while(running && rs.next()) {
          long id=rs.getLong("id"); String name=rs.getString("name"); java.math.BigDecimal amount=rs.getBigDecimal("amount");
          if(amount==null) throw new IllegalStateException("Null amount for id="+id);
          String decision = checkFraud(id,amount);
          String topic="APPROVED".equals(decision) ? "approved-payments" : "rejected-payments";
          String json="{\"id\":"+id+",\"name\":\""+escape(name)+"\",\"amount\":"+amount.toPlainString()+",\"status\":\""+decision+"\"}";
          Map<String,Long> offset=Collections.singletonMap("last_id",id);
          records.add(new SourceRecord(PARTITION,offset,topic,null,Schema.STRING_SCHEMA,Long.toString(id),Schema.STRING_SCHEMA,json));
          lastId=id;
        }
      }
    } catch(Exception e) {
      // Do not advance past a failed row. Any successful records in this batch
      // may be retried, so consumers must deduplicate using payment ID.
      System.err.println("Fraud source poll failed; will retry: "+e);
      if(records.isEmpty()) Thread.sleep(interval);
    }
    if(records.isEmpty()) Thread.sleep(interval);
    return records;
  }
  private String checkFraud(long id,java.math.BigDecimal amount) throws IOException {
    HttpURLConnection c=(HttpURLConnection)new URL(apiUrl).openConnection();
    try {
      c.setRequestMethod("POST"); c.setConnectTimeout(2000); c.setReadTimeout(3000);
      c.setDoOutput(true); c.setRequestProperty("Content-Type","application/json");
      byte[] data=("{\"transactionId\":"+id+",\"amount\":"+amount.toPlainString()+"}").getBytes(StandardCharsets.UTF_8);
      try(OutputStream out=c.getOutputStream()){out.write(data);}
      int code=c.getResponseCode(); if(code!=200) throw new IOException("Fraud API HTTP "+code);
      String response;
      try(InputStream in=c.getInputStream(); ByteArrayOutputStream out=new ByteArrayOutputStream()) {
        byte[] buf=new byte[1024]; int n; while((n=in.read(buf))!=-1){out.write(buf,0,n); if(out.size()>16384) throw new IOException("API response too large");}
        response=new String(out.toByteArray(),StandardCharsets.UTF_8);
      }
      Matcher m=Pattern.compile("\\\"status\\\"\\s*:\\s*\\\"(APPROVED|REJECTED)\\\"").matcher(response);
      if(!m.find()) throw new IOException("Unexpected API response: "+response);
      return m.group(1);
    } finally {c.disconnect();}
  }
  private static String escape(String s) {
    if(s==null) return "";
    StringBuilder b=new StringBuilder();
    for(char ch:s.toCharArray()) {switch(ch) {case '\\': b.append("\\\\"); break; case '"': b.append("\\\""); break; case '\n': b.append("\\n"); break; case '\r': b.append("\\r"); break; case '\t': b.append("\\t"); break; default: if(ch<32) b.append(String.format("\\u%04x",(int)ch)); else b.append(ch);}}
    return b.toString();
  }
  public void stop(){running=false;}
  public String version(){return "1.0.0";}
}
