
# lindorm-tsdb-client


## Adding the library to your project
   
The library artifact is published in Maven central, available at [Lindorm TSDB Client](https://mvnrepository.com/artifact/com.aliyun.lindorm/lindorm-tsdb-client).


### Release versions

Maven dependency:

```xml
<dependency>
  <groupId>com.aliyun.lindorm</groupId>
  <artifactId>lindorm-tsdb-client</artifactId>
  <version>1.0.5</version>
</dependency>
```


## Quick start

```Java
import com.aliyun.lindorm.tsdb.client.ClientOptions;
import com.aliyun.lindorm.tsdb.client.LindormTSDBClient;
import com.aliyun.lindorm.tsdb.client.LindormTSDBFactory;
import com.aliyun.lindorm.tsdb.client.exception.LindormTSDBException;
import com.aliyun.lindorm.tsdb.client.model.QueryResult;
import com.aliyun.lindorm.tsdb.client.model.Record;
import com.aliyun.lindorm.tsdb.client.model.ResultSet;
import com.aliyun.lindorm.tsdb.client.model.WriteResult;
import com.aliyun.lindorm.tsdb.client.utils.ExceptionUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * @author jianhong.hjh
 * @date 2021-12-21 15:39
 */
public class QuickStart {

    public static void main(String[] args) {

        // 1.创建客户端实例
        String url = "http://ld-xxxx-proxy-tsdb-pub.lindorm.rds.aliyuncs.com:8242";
        // LindormTSDBClient线程安全，可以重复使用，无需频繁创建和销毁
        ClientOptions options = ClientOptions.newBuilder(url).build();
        LindormTSDBClient lindormTSDBClient = LindormTSDBFactory.connect(options);

        // 2.创建数据库demo和表sensor
        lindormTSDBClient.execute("CREATE DATABASE demo");
        lindormTSDBClient.execute("demo","CREATE TABLE sensor (device_id VARCHAR TAG,region VARCHAR TAG,time BIGINT,temperature DOUBLE,humidity DOUBLE,PRIMARY KEY(device_id))");

        // 3.写入数据
        int numRecords = 10;
        List<Record> records = new ArrayList<>(numRecords);
        long currentTime = System.currentTimeMillis();
        for (int i = 0; i < numRecords; i++) {
            Record record = Record
                    .table("sensor")
                    .time(currentTime + i * 1000)
                    .tag("device_id", "F07A1260")
                    .tag("region", "north-cn")
                    .addField("temperature", 12.1 + i)
                    .addField("humidity", 45.0 + i)
                    .build();
            records.add(record);
        }

        CompletableFuture<WriteResult> future = lindormTSDBClient.write("demo", records);
        // 处理异步写入结果
        future.whenComplete((r, ex) -> {
            // 处理写入失败
            if (ex != null) {
                System.out.println("Failed to write.");
                Throwable throwable = ExceptionUtils.getRootCause(ex);
                if (throwable instanceof LindormTSDBException) {
                    LindormTSDBException e = (LindormTSDBException) throwable;
                    System.out.println("Caught an LindormTSDBException, which means your request made it to Lindrom TSDB, "
                            + "but was rejected with an error response for some reason.");
                    System.out.println("Error Code: " + e.getCode());
                    System.out.println("SQL State:  " + e.getSqlstate());
                    System.out.println("Error Message: " + e.getMessage());
                }  else {
                    throwable.printStackTrace();
                }
            } else  {
                System.out.println("Write successfully.");
            }
        });
        // 这里作为示例, 简单同步等待
        System.out.println(future.join());

        // 4.查询数据
        String sql = "select * from sensor limit 10";
        ResultSet resultSet = lindormTSDBClient.query("demo", sql);

        try {
            // 处理查询结果
            QueryResult result = null;
            while ((result = resultSet.next()) != null) {
                List<String> columns = result.getColumns();
                System.out.println("columns: " + columns);
                List<String> metadata = result.getMetadata();
                System.out.println("metadata: " + metadata);
                List<List<Object>> rows = result.getRows();
                for (int i = 0, size = rows.size(); i < size; i++) {
                    List<Object> row = rows.get(i);
                    System.out.println("row #" + i + " : " + row);
                }
            }
        } finally {
            resultSet.close();
        }

        lindormTSDBClient.shutdown();
    }
}
 ```
