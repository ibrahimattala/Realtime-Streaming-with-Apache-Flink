

 package FlinkCommerce;

 import Deserializer.JSONValueDeserializationSchema;
 import Dto.SalesPerCategory;
 import Dto.SalesPerDay;
 import Dto.SalesPerMonth;
 import Dto.Transaction;
 import org.apache.flink.api.common.eventtime.WatermarkStrategy;
 import org.apache.flink.api.connector.sink.Sink;
 import org.apache.flink.connector.elasticsearch.sink.Elasticsearch7SinkBuilder;
 import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
 import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
 import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
 import org.apache.flink.connector.kafka.source.KafkaSource;
 import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
 import org.apache.flink.elasticsearch7.shaded.org.apache.http.HttpHost;
 import org.apache.flink.elasticsearch7.shaded.org.elasticsearch.action.index.IndexRequest;
 import org.apache.flink.elasticsearch7.shaded.org.elasticsearch.client.Requests;
 import org.apache.flink.elasticsearch7.shaded.org.elasticsearch.common.xcontent.XContentType;
 import org.apache.flink.streaming.api.datastream.DataStream;
 import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
 import org.apache.flink.connector.jdbc.JdbcSink;
 
 import java.sql.Date;
 
 import static utils.JsonUtil.convertTransactionToJson;
 
 public class DataStreamJob {
     private static final String jdbcUrl = "jdbc:postgresql://localhost:5432/postgres";
     private static final String username = "postgres";
     private static final String password = "postgres";
 
     public static void main(String[] args) throws Exception {
             // Sets up the execution environment, which is the main entry point
             // to building Flink applications.
             final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
 
            String topic = "financial_transactions";
 
             KafkaSource<Transaction> source = KafkaSource.<Transaction>builder()
                 .setBootstrapServers("localhost:9092")
                 .setTopics(topic)
                 .setGroupId("flink-group")
                 .setStartingOffsets(OffsetsInitializer.earliest())
                 .setValueOnlyDeserializer(new JSONValueDeserializationSchema())
                 .build();

            DataStream<Transaction> transactionStream = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka source");

            transactionStream.print();

            JdbcExecutionOptions execOptions = new JdbcExecutionOptions.Builder()
                .withBatchSize(1000)
                .withBatchIntervalMs(200)
                .withMaxRetries(5)
                .build();

            JdbcConnectionOptions connOptions = new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                .withUrl(jdbcUrl)
                .withDriverName("org.postgresql.Driver")
                .withUsername(username)
                .withPassword(password)
                .build();
 
                //create transactions table
            transactionStream.addSink(JdbcSink.sink(
                    "CREATE TABLE IF NOT EXISTS transactions (" +
                            "transaction_id VARCHAR(255) PRIMARY KEY, " +
                            "product_id VARCHAR(255), " +
                            "product_name VARCHAR(255), " +
                            "product_category VARCHAR(255), " +
                            "product_price DOUBLE PRECISION, " +
                            "product_quantity INTEGER, " +
                            "product_brand VARCHAR(255), " +
                            "total_amount DOUBLE PRECISION, " +
                            "currency VARCHAR(255), " +
                            "customer_id VARCHAR(255), " +
                            "transaction_date TIMESTAMP, " +
                            "payment_method VARCHAR(255) " +
                            ")",
                    (JdbcStatementBuilder<Transaction>) (preparedStatement, transaction) -> {
    
                    },
                    execOptions,
                    connOptions
                )).name("Create Transactions Table Sink");
         
 
 
         // Execute program, beginning computation.
         env.execute("Flink Ecommerce Realtime Streaming");
     }
 }
 