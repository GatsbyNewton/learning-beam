package edu.wzm.operator;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.RowCoder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.transforms.Convert;
import org.apache.beam.sdk.schemas.transforms.Join;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.JsonToRow;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.*;

public class JoinDemo implements Callable<String>, Serializable {
    public static final Logger LOGGER = LogManager.getLogger(JoinDemo.class);

    public static final Map<String, String> map = new ConcurrentHashMap<>();

    @Override
    public String call() {
        JSONArray currArray = JSON.parseArray(curr);
        // TODO 数据解析
        JSONArray baseArray = JSON.parseArray(base);
        Set<String> columnNames = currArray.getJSONObject(0).keySet();
        JoinDemo transformer = new JoinDemo();
        transformer.run(currArray, baseArray, columnNames, "z=x+y", "where a=b", Thread.currentThread().getName());
        return Thread.currentThread().getName();
    }

    public Schema.Builder createSchema(Set<String> columnNames) {
        Schema.Builder schemaBuilder = Schema.builder();
        for (String name : columnNames) {
            schemaBuilder.addStringField(name);
        }

        Schema schema = schemaBuilder.build();
        return schemaBuilder;
    }

    public List<String> genData(JSONArray array) {
        return array.toJavaList(String.class);
    }

    public Schema addField(Schema.Builder builder) {
        return builder
//                .addStringField("app_id")
//                .addStringField("where")
                .build();
    }

    public void run(JSONArray currArray, JSONArray baseArray, Set<String> columnuNames,
                    String formula, String where, String threadName) {
        Schema.Builder schemaBuilder = createSchema(columnuNames);
        Schema schema = addField(schemaBuilder);

        /**
         * age name
         * age name base_age base_name
         */
        Set<String> joinColumnNames = new HashSet<>(columnuNames);
        for (String name : columnuNames) {
            joinColumnNames.add("base_" + name);
        }

        Schema.Builder joinSchemaBuilder = createSchema(joinColumnNames);
        Schema joinSchema = addField(joinSchemaBuilder);

        PipelineOptions options = PipelineOptionsFactory.create();
        Pipeline pipeline = Pipeline.create(options);
        PCollection<Row> currColl = pipeline.apply(Create.of(genData(currArray)))
                .apply(JsonToRow.withSchema(schema))
                .apply(Convert.toRows());
        PCollection<Row> baseColl = pipeline.apply(Create.of(genData(baseArray)))
                .apply(JsonToRow.withSchema(schema))
                .apply(Convert.toRows());

//        PCollectionList<Row> collectionList = PCollectionList.of(currColl).and(baseColl);
//        collectionList.apply(Flatten.<Row>pCollections())
//                .apply("ConcatResultKVs", MapElements.via( // 拼接最后的格式化输出（Key为Word，Value为Count）
//                        new SimpleFunction<Row, String>() {
//
//                            @Override
//                            public String apply(Row input) {
//                                System.out.println("input: " + input);
//                                return input.getString(1);
//                            }
//
//                        }));

        List<String> list = new ArrayList<>();
        PCollection<Row> joinColl = currColl.apply("inner join", Join.<Row, Row>innerJoin(baseColl).using("app_id"))
                .apply(ParDo.of(new InnerJoinFunc(list, joinSchema, threadName)))
                .setCoder(RowCoder.of(joinSchema));
//        joinColl.setRowSchema(joinSchema);

        pipeline.run().waitUntilFinish();
    }

    static ExecutorService service = Executors.newFixedThreadPool(10);
    public static void main(String[] args) {
//        JSONArray currArray = JSON.parseArray(curr);
//        JSONArray baseArray = JSON.parseArray(base);
//        Set<String> columnNames = currArray.getJSONObject(0).keySet();
//        JsonTransformer transformer = new JsonTransformer();
//        transformer.run(currArray, baseArray, columnNames, "z=x+y", "where a=b");

        List<Future<String>> futures = new ArrayList<>();
        for (int i = 0; i < 1; i++) {
            Future<String> future = service.submit(new JoinDemo());
            futures.add(future);
        }

        try {
            for (Future<String> future : futures) {
                String name = future.get();
                System.out.println(name + "# " + map.get(name));
            }
        } catch (Exception e) {

        }
    }

    public static class InnerJoinFunc extends DoFn<Row, Row> implements Serializable{
        private List<String> list;
        private Schema joinSchema;
        private String threadName;

        public InnerJoinFunc(List<String> list, Schema joinSchema, String threadName) {
            this.list = list;
            this.joinSchema = joinSchema;
            this.threadName = threadName;
        }

        @ProcessElement
        public void processElement(ProcessContext c) {
            Row row = c.element();
//            System.out.println("row: " + row);
            Row left = row.getRow(0);
            Row right = row.getRow(1);
            Row.Builder rowBuilder = Row.withSchema(joinSchema);
            for (int i = 0, len = joinSchema.getFieldCount(); i < len; i++) {
                if (left.getSchema().hasField(joinSchema.getField(i).getName())) {
//                    rowBuilder.withFieldValue(joinSchema.getField(i).getName(),
//                            left.getString(joinSchema.getField(i).getName()));

                    rowBuilder.addValues(
                            left.getString(joinSchema.getField(i).getName()));
                } else {
                    rowBuilder.addValues(
                            right.getString(joinSchema.getField(i).getName().replace("base_", "")));
                }
            }

            Row newRow = rowBuilder.build();
            Random random = new Random();
            int index = random.nextInt(10);
            map.put(threadName, index + ": " + newRow.getString(index));
            System.out.println(map);
//            try {
//                Thread.sleep(index * 20);
//            } catch (InterruptedException e) {
//
//            }
            c.output(newRow);
        }

        @Teardown
        public void close() {
        }

    }


    public static class FlatJoinFunc extends DoFn<Row, Row> implements Serializable{
        private Schema schema;
        private List<String> onCol;

        public FlatJoinFunc(Schema schema, List<String> onCol) {
            this.schema = schema;
            this.onCol = onCol;
        }

        @ProcessElement
        public void processElement(ProcessContext c) {
            Row row = c.element();
            for (int i = 0, count = row.getFieldCount(); i < count; i++) {

            }
        }
    }

    private static String curr = "[   \n" +
            "    {\n" +
            "        \"app_id\": \"001\",\n" +
            "        \"order_num\": \"101\",\n" +
            "        \"dau\": \"102\",\n" +
            "        \"deal_user_cnt\": \"103\",\n" +
            "        \"dt\": \"0201\"\n" +
            "    }\n" +
            "]";

    private static String base = "[   \n" +
            "    {\n" +
            "        \"app_id\": \"001\",\n" +
            "        \"dau\": \"101\",\n" +
            "        \"order_num\": \"102\",\n" +
            "        \"deal_user_cnt\": \"103\",\n" +
            "        \"dt\": \"0202\"\n" +
            "    }\n" +
            "]";

//            "   \n" +
//            "    [\n" +
//            "        {\n" +
//            "            \"column\": \"app_id\",\n" +
//            "            \"value\": \"001\"\n" +
//            "        },\n" +
//            "        {\n" +
//            "            \"column\": \"dau\",\n" +
//            "            \"value\": \"101\"\n" +
//            "        },\n" +
//            "        {\n" +
//            "            \"column\": \"order_num\",\n" +
//            "            \"value\": \"102\"\n" +
//            "        },\n" +
//            "        {\n" +
//            "            \"column\": \"deal_user_cnt\",\n" +
//            "            \"value\": \"103\"\n" +
//            "        },\n" +
//            "        {\n" +
//            "            \"column\": \"dt\",\n" +
//            "            \"value\": \"0201\"\n" +
//            "        }\n" +
//            "    ]\n" +
//            "";



//            "   \n" +
//            "    [\n" +
//            "        {\n" +
//            "            \"column\": \"app_id\",\n" +
//            "            \"value\": \"001\"\n" +
//            "        },\n" +
//            "        {\n" +
//            "            \"column\": \"dau\",\n" +
//            "            \"value\": \"101\"\n" +
//            "        },\n" +
//            "        {\n" +
//            "            \"column\": \"order_num\",\n" +
//            "            \"value\": \"102\"\n" +
//            "        },\n" +
//            "        {\n" +
//            "            \"column\": \"deal_user_cnt\",\n" +
//            "            \"value\": \"103\"\n" +
//            "        },\n" +
//            "        {\n" +
//            "            \"column\": \"dt\",\n" +
//            "            \"value\": \"0201\"\n" +
//            "        }\n" +
//            "    ]\n" +
//            "";
}
