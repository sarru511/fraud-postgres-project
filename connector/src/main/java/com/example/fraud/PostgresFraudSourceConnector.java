package com.example.fraud;

import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.source.SourceConnector;
import org.apache.kafka.common.config.ConfigDef;
import java.util.*;

public class PostgresFraudSourceConnector extends SourceConnector {
  private Map<String,String> settings;
  public void start(Map<String,String> props) { this.settings = new HashMap<>(props); }
  public Class<? extends Task> taskClass() { return PostgresFraudSourceTask.class; }
  public List<Map<String,String>> taskConfigs(int maxTasks) { return Collections.singletonList(new HashMap<>(settings)); }
  public void stop() { }
  public String version() { return "1.0.0"; }
  public ConfigDef config() {
    return new ConfigDef()
      .define("jdbc.url",ConfigDef.Type.STRING,ConfigDef.Importance.HIGH,"PostgreSQL JDBC URL")
      .define("jdbc.user",ConfigDef.Type.STRING,ConfigDef.Importance.HIGH,"Database user")
      .define("jdbc.password",ConfigDef.Type.PASSWORD,ConfigDef.Importance.HIGH,"Database password")
      .define("fraud.api.url",ConfigDef.Type.STRING,ConfigDef.Importance.HIGH,"Fraud API endpoint")
      .define("poll.interval.ms",ConfigDef.Type.INT,3000,ConfigDef.Importance.MEDIUM,"Polling interval")
      .define("batch.size",ConfigDef.Type.INT,100,ConfigDef.Importance.MEDIUM,"Max rows per poll");
  }
}
