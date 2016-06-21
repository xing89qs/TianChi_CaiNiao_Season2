package my.group.GradientBoostingRegression;

import com.aliyun.odps.Odps;
import com.aliyun.odps.account.Account;
import com.aliyun.odps.account.AliyunAccount;
import com.aliyun.odps.data.TableInfo;
import com.aliyun.odps.mapred.JobClient;
import com.aliyun.odps.mapred.Mapper;
import com.aliyun.odps.mapred.Reducer;
import com.aliyun.odps.mapred.conf.JobConf;
import com.aliyun.odps.mapred.conf.JobConf.SortOrder;
import com.aliyun.odps.mapred.conf.SessionState;
import com.aliyun.odps.mapred.utils.InputUtils;
import com.aliyun.odps.mapred.utils.OutputUtils;
import com.aliyun.odps.mapred.utils.SchemaUtils;
import org.apache.commons.digester3.Digester;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.time.DateUtils;

import java.io.InputStream;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Mapreduce任务的主程序。
 * 注意：此文件只供本地测试使用，所有修改不会反映到最终线上实际执行的JobLauncher。
 * 所有对mapreduce进行的配置请在main/resources/META-INF/base.mapred.xml中完成
 */
@SuppressWarnings("rawtypes")
public class JobLauncher {
    /**
     * 表示ODPS表的pojo类
     */
    public static class OdpsTableInfo {
        private String name;
        private List<String> partitions = new ArrayList<String>();

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public List<String> getPartitions() {
            return partitions;
        }

        public void setPartitions(List<String> partitions) {
            this.partitions = partitions;
        }

        public void addPartition(String partition) {
            partitions.add(partition);
        }
    }

    /**
     * 表示mapreduce程序配置的pojo类
     */
    public static class MapreduceConfigInfo {
        private String baseId;
        private String projectId;
        private String resourceName;
        private String idePath;

        private String mapOutputKey;
        private String mapOutputValue;
        private String partitionColumns;
        private String outputKeySortColumns;
        private String outputKeySortOrders;
        private String outputGroupingColumns;

        private int numReduceTask;
        private int memoryForMapTask;
        private int memoryForReduceTask;

        private String jobLauncher;
        private String mapper;
        private String reducer;
        private String combiner;

        private List<OdpsTableInfo> inputTables = new ArrayList<OdpsTableInfo>();
        private OdpsTableInfo outputTable;

        public String getBaseId() {
            return baseId;
        }

        public void setBaseId(String baseId) {
            this.baseId = baseId;
        }

        public String getProjectId() {
            return projectId;
        }

        public void setProjectId(String projectId) {
            this.projectId = projectId;
        }

        public String getResourceName() {
            return resourceName;
        }

        public void setResourceName(String resourceName) {
            this.resourceName = resourceName;
        }

        public String getIdePath() {
            return idePath;
        }

        public void setIdePath(String idePath) {
            this.idePath = idePath;
        }

        public String getMapOutputKey() {
            return mapOutputKey;
        }

        public void setMapOutputKey(String mapOutputKey) {
            this.mapOutputKey = mapOutputKey;
        }

        public String getMapOutputValue() {
            return mapOutputValue;
        }

        public void setMapOutputValue(String mapOutputValue) {
            this.mapOutputValue = mapOutputValue;
        }

        public String getPartitionColumns() {
            return partitionColumns;
        }

        public void setPartitionColumns(String partitionColumns) {
            this.partitionColumns = partitionColumns;
        }

        public String getOutputKeySortColumns() {
            return outputKeySortColumns;
        }

        public void setOutputKeySortColumns(String outputKeySortColumns) {
            this.outputKeySortColumns = outputKeySortColumns;
        }

        public String getOutputKeySortOrders() {
            return outputKeySortOrders;
        }

        public void setOutputKeySortOrders(String outputKeySortOrders) {
            this.outputKeySortOrders = outputKeySortOrders;
        }

        public String getOutputGroupingColumns() {
            return outputGroupingColumns;
        }

        public void setOutputGroupingColumns(String outputGroupingColumns) {
            this.outputGroupingColumns = outputGroupingColumns;
        }

        public int getNumReduceTask() {
            return numReduceTask;
        }

        public void setNumReduceTask(int numReduceTask) {
            this.numReduceTask = numReduceTask;
        }

        public int getMemoryForMapTask() {
            return memoryForMapTask;
        }

        public void setMemoryForMapTask(int memoryForMapTask) {
            this.memoryForMapTask = memoryForMapTask;
        }

        public int getMemoryForReduceTask() {
            return memoryForReduceTask;
        }

        public void setMemoryForReduceTask(int memoryForReduceTask) {
            this.memoryForReduceTask = memoryForReduceTask;
        }

        public String getJobLauncher() {
            return jobLauncher;
        }

        public void setJobLauncher(String jobLauncher) {
            this.jobLauncher = jobLauncher;
        }

        public String getMapper() {
            return mapper;
        }

        public void setMapper(String mapper) {
            this.mapper = mapper;
        }

        public String getReducer() {
            return reducer;
        }

        public void setReducer(String reducer) {
            this.reducer = reducer;
        }

        public String getCombiner() {
            return combiner;
        }

        public void setCombiner(String combiner) {
            this.combiner = combiner;
        }

        public List<OdpsTableInfo> getInputTables() {
            return inputTables;
        }

        public void setInputTables(List<OdpsTableInfo> inputTables) {
            this.inputTables = inputTables;
        }

        public OdpsTableInfo getOutputTable() {
            return outputTable;
        }

        public void setOutputTable(OdpsTableInfo outputTable) {
            this.outputTable = outputTable;
        }

        public void addInputTable(OdpsTableInfo inputTable) {
            inputTables.add(inputTable);
        }
    }

    /**
     * 以给定日期为基准展开宏
     *
     * @param str     含有待展开宏的字符串
     * @param dateYmd 基准日期，对应{yyyymmdd}，{yyyymmdd-n}，{yyyymmdd+n}
     * @return 展开后的字符串
     */
    public static String expandMacroDateYmd(String str, Date dateYmd) {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
        Pattern dateYmdPat = Pattern.compile("\\{\\s*yyyymmdd\\s*(([+-])\\s*(\\d+))?\\s*\\}");
        String res = str;
        Matcher matcher = dateYmdPat.matcher(res);
        while (matcher.find()) {
            String expr = matcher.group(1);
            String op = matcher.group(2);
            String ndays = matcher.group(3);

            if (StringUtils.isEmpty(expr)) {
                // 无日期计算部分，直接展开
                res = matcher.replaceFirst(sdf.format(dateYmd));
                matcher = dateYmdPat.matcher(res);
            } else if ("+".equals(op)) {
                // 基准日期+n 天
                int n = Integer.parseInt(ndays);
                res = matcher.replaceFirst(sdf.format(DateUtils.addDays(dateYmd, n)));
                matcher = dateYmdPat.matcher(res);
            } else if ("-".equals(op)) {
                // 基准日期-n 天
                int n = -Integer.parseInt(ndays);
                res = matcher.replaceFirst(sdf.format(DateUtils.addDays(dateYmd, n)));
                matcher = dateYmdPat.matcher(res);
            }
        }
        return res;
    }

    /**
     * 解析base.mapred.xml配置信息到java对象
     *
     * @return 配置信息pojo对象
     */
    public static MapreduceConfigInfo parseConfig(String extraPartitions) {
        Digester digester = new Digester();
        digester.setValidating(false);

        digester.addObjectCreate("mapred", MapreduceConfigInfo.class);
        digester.addBeanPropertySetter("mapred/baseId");
        digester.addBeanPropertySetter("mapred/projectId");
        digester.addBeanPropertySetter("mapred/resourceName");
        digester.addBeanPropertySetter("mapred/idePath");

        digester.addBeanPropertySetter("mapred/mapOutputKey");
        digester.addBeanPropertySetter("mapred/mapOutputValue");
        digester.addBeanPropertySetter("mapred/partitionColumns");
        digester.addBeanPropertySetter("mapred/outputKeySortColumns");
        digester.addBeanPropertySetter("mapred/outputKeySortOrders");
        digester.addBeanPropertySetter("mapred/outputGroupingColumns");
        digester.addBeanPropertySetter("mapred/numReduceTask");
        digester.addBeanPropertySetter("mapred/memoryForMapTask");
        digester.addBeanPropertySetter("mapred/memoryForReduceTask");

        digester.addBeanPropertySetter("mapred/jobLauncher");
        digester.addBeanPropertySetter("mapred/mapper");
        digester.addBeanPropertySetter("mapred/reducer");
        digester.addBeanPropertySetter("mapred/combiner");

        digester.addObjectCreate("mapred/inputTables/table", OdpsTableInfo.class);
        digester.addBeanPropertySetter("mapred/inputTables/table/name");
        digester.addCallMethod("mapred/inputTables/table/partitions/partition", "addPartition", 1);
        digester.addCallParam("mapred/inputTables/table/partitions/partition", 0);
        digester.addSetNext("mapred/inputTables/table", "addInputTable");

        digester.addObjectCreate("mapred/outputTable", OdpsTableInfo.class);
        digester.addBeanPropertySetter("mapred/outputTable/name");
        digester.addCallMethod("mapred/outputTable/partition", "addPartition", 1);
        digester.addCallParam("mapred/outputTable/partition", 0);
        digester.addSetNext("mapred/outputTable", "setOutputTable");

        InputStream is = ClassLoader.getSystemResourceAsStream("META-INF/base.mapred.xml");
        try {
            MapreduceConfigInfo conf = digester.parse(is);

            // 将额外分区合并入输入表和输出表
            if (!extraPartitions.isEmpty()) {
                String[] eps = extraPartitions.split(":");
                for (String ep : eps) {
                    int pos = ep.indexOf("/");
                    String tableName = ep.substring(0, pos);
                    String partition = ep.substring(pos + 1);

                    for (OdpsTableInfo t : conf.getInputTables()) {
                        if (t.getName().equals(tableName)) {
                            t.addPartition(partition);
                        }
                    }

                    if (conf.getOutputTable().getName().equals(tableName)) {
                        conf.getOutputTable().addPartition(partition);
                    }
                }
            }

            return conf;
        } catch (Exception e) {
            return null;
        }
    }

    /**
     * 通过配置和运行时参数构建JobConf
     *
     * @param conf    程序配置
     * @param dateYmd 基准时间
     * @return JobConf对象
     * @throws Exception
     */
    @SuppressWarnings("unchecked")
    public static JobConf makeMapreduceJobConf(MapreduceConfigInfo conf, Date dateYmd) throws Exception {
        JobConf job = new JobConf();

        if (conf == null) {
            throw new Exception("Parse base.mapred.xml failed!");
        }
        if (conf.getMapper() == null || conf.getMapper().isEmpty()) {
            throw new Exception("No mapper class specified");
        }

        // 设置mapper
        String mapperClassName = conf.getMapper();
        Class<Mapper> mapperClz = (Class<Mapper>) Class.forName(mapperClassName);
        job.setMapperClass(mapperClz);

        // 增加可选的reducer
        if (conf.getReducer() != null && !conf.getReducer().isEmpty()) {
            String reducerClassName = conf.getReducer();
            Class<Reducer> reducerClz = (Class<Reducer>) Class.forName(reducerClassName);
            job.setReducerClass(reducerClz);
        }
        // 增加可选的combiner
        if (conf.getCombiner() != null && !conf.getCombiner().isEmpty()) {
            String combinerClassName = conf.getCombiner();
            Class<Reducer> combinerClz = (Class<Reducer>) Class.forName(combinerClassName);
            job.setCombinerClass(combinerClz);
        }

        // 设置task
        if (conf.getMapOutputKey() == null || conf.getMapOutputKey().isEmpty()) {
            throw new Exception("No mapOutputValue specified");
        }
        if (conf.getMapOutputValue() == null || conf.getMapOutputValue().isEmpty()) {
            throw new Exception("No mapOutputValue specified");
        }
        job.setMapOutputKeySchema(SchemaUtils.fromString(conf.getMapOutputKey()));
        job.setMapOutputValueSchema(SchemaUtils.fromString(conf.getMapOutputValue()));

        if (conf.getPartitionColumns() != null && !conf.getPartitionColumns().isEmpty()) {
            job.setPartitionColumns(conf.getPartitionColumns().split(","));
        }
        if (conf.getOutputKeySortColumns() != null && !conf.getOutputKeySortColumns().isEmpty()) {
            job.setOutputKeySortColumns(conf.getOutputKeySortColumns().split(","));
        }
        if (conf.getOutputKeySortOrders() != null
                && !conf.getOutputKeySortOrders().isEmpty()) {
            String[] orders = conf.getOutputKeySortOrders().split(",");
            SortOrder[] sortOrders = new SortOrder[orders.length];
            for (int i = 0; i < orders.length; i++) {
                String order = orders[i].trim().toLowerCase();
                sortOrders[i] = order.equals("desc") ? SortOrder.DESC
                        : SortOrder.ASC;
            }
            job.setOutputKeySortOrder(sortOrders);
        }
        if (conf.getOutputGroupingColumns() != null && !conf.getOutputGroupingColumns().isEmpty()) {
            job.setOutputGroupingColumns(conf.getOutputGroupingColumns().split(","));
        }
        if (conf.getNumReduceTask() >= 0) {
            job.setNumReduceTasks(conf.getNumReduceTask());
        }
        if (conf.getMemoryForMapTask() >= 0) {
            job.setMemoryForMapperJVM(conf.getMemoryForMapTask());
        }
        if (conf.getMemoryForReduceTask() >= 0) {
            job.setMemoryForReducerJVM(conf.getMemoryForReduceTask());
        }

        // 设置输入表
        for (OdpsTableInfo it : conf.getInputTables()) {
            if (it.getPartitions() == null || it.getPartitions().size() == 0) {
                InputUtils.addTable(TableInfo.builder().tableName(it.getName()).build(), job);
            } else {
                for (String p : it.getPartitions()) {
                    InputUtils.addTable(TableInfo.builder().tableName(it.getName()).partSpec(expandMacroDateYmd(p, dateYmd)).build(), job);
                }
            }
        }

        // 设置输出表
        if (conf.getOutputTable() == null) {
            throw new Exception("No output table specified");
        }
        if (conf.getOutputTable().getPartitions() != null && conf.getOutputTable().getPartitions().size() > 1) {
            throw new Exception("Output table can not has multiple partitions");
        }
        if (conf.getOutputTable().getPartitions() == null || conf.getOutputTable().getPartitions().size() == 0) {
            OutputUtils.addTable(TableInfo.builder().tableName(conf.getOutputTable().getName()).build(), job);
        } else {
            OutputUtils.addTable(TableInfo.builder().tableName(conf.getOutputTable()
                    .getName())
                    .partSpec(expandMacroDateYmd(conf.getOutputTable().getPartitions().get(0), dateYmd)).build(), job);
        }

        return job;
    }

    public static void main(String[] args) throws Exception {
        String testDateYmd = "20140102";
        String extraPartitions = "";

        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
        MapreduceConfigInfo conf = parseConfig(extraPartitions);
        JobConf job = makeMapreduceJobConf(conf, sdf.parse(testDateYmd));
        if (job == null) {
            throw new Exception("Create mapreduce job failed");
        }

        Account account = new AliyunAccount("x", "x");
        Odps odps = new Odps(account);
        odps.setDefaultProject("local");
        SessionState sessionState = SessionState.get();
        sessionState.setLocalRun(true);
        sessionState.setOdps(odps);

        JobClient.runJob(job);
    }
}
