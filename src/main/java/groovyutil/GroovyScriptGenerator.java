package groovyutil;


import org.apache.commons.lang3.ArrayUtils;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @ProjectName: yqcloud
 * @Description: 生成groovy数据库表脚本 适用mysql数据库
 * @Author: WeiLingYun
 * @CreateDate: 2018/12/10 11:18
 * @Version: 1.0.0
 */
public class GroovyScriptGenerator {

    /**
     * 指定脚本包名
     */
    private String scriptOutPath;
    /**
     * 作者名字
     */
    private String scriptAuthor;

    /**
     * 表名
     */
    private String tableName;

    /**
     * 脚本生成的目的地
     */
    private String targetPath;
    /**
     * 列名数组
     */
    private String[] columnNames;
    /**
     * 列类型数组
     */
    private String[] columnTypes;
    /**
     * 列大小数组
     */
    private Integer[] columnSizes;
    /**
     * 列是否必填数组
     */
    private Integer[] columnRequireds;

    /**
     * 列是否自增
     */
    private String[] columnAutoIncrementSigns;

    /**
     * 列描述数组
     */
    private String[] columnRemarks;
    /**
     * 列默认值
     */
    private Object[] columnDefaultValues;


    /**
     * 数据库连接
     */
    private String url;
    private String name;
    private String password;
    private String driver;

    public GroovyScriptGenerator() {
    }

    public void generateGroovyScript(String db) {
        //创建连接
        Connection con = null;

        try {
            try {
                Class.forName(driver);
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
            }
            con = DriverManager.getConnection(url, name, password);
            DatabaseMetaData metaData = con.getMetaData();
            //表的主键信息
            ResultSet pk = metaData.getPrimaryKeys(null, null, tableName);
            Set<String> pkColumn = new HashSet<>();
            while (pk.next()) {
                pkColumn.add(pk.getObject("COLUMN_NAME").toString());
            }
            //表的索引信息
            Map<String, List<IndexInfo>> indexMap = obtainIndexParam(metaData);
            //表的列信息
            Map<String, Object[]> tableParam = obtainColumnParam(metaData);
            //拼接groovy脚本
            String content = parse((int) tableParam.get("row")[0], tableParam, indexMap, pkColumn);
            try {
                File file = new File(targetPath);
                if (!file.exists()) {
                    file.mkdirs();
                } else {
                    File[] files = file.listFiles();
                    if (files != null) {
                        for (int i = 0; i < file.length(); i++) {
                            files[i].delete();
                        }
                    }
                }
                FileWriter fw = new FileWriter(new File(targetPath) + "/" + tableName + ".groovy");
                PrintWriter pw = new PrintWriter(fw);
                pw.println(content);
                pw.flush();
                pw.close();
                System.out.println("Database[" + db + "] : Table[ " + tableName + " ] : The groovy script was " +
                        "generated successfully! Please check the directory[" + targetPath + "]");
            } catch (IOException e) {
                e.printStackTrace();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            try {
                con.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    private Map<String, Object[]> obtainColumnParam(DatabaseMetaData metaData) throws SQLException {
        //先获取列数
        ResultSet column = metaData.getColumns(null, null, tableName, null);
        int row = 0;
        while (column.next()) {
            ++row;
        }
        int i = 0;
        columnNames = new String[row];
        columnTypes = new String[row];
        columnSizes = new Integer[row];
        columnRequireds = new Integer[row];
        columnAutoIncrementSigns = new String[row];
        columnRemarks = new String[row];
        columnDefaultValues = new Object[row];
        //上面的column.next()操作将column（ResultSet）的指针指向了最后，所以需要再次执行方法查询出列
        ResultSet columns = metaData.getColumns(null, null, tableName, null);
        while (columns.next()) {
            columnNames[i] = columns.getString("COLUMN_NAME");
            String typeName = columns.getString("TYPE_NAME");
            typeName = typeName.toLowerCase();
            if (typeName.startsWith("bit")) {
                columnTypes[i] = "tinyint";
            } else {
                columnTypes[i] = typeName;
            }
            columnSizes[i] = StringUtils.isEmpty(columns.getString("COLUMN_SIZE")) ? 0 : Integer.valueOf(columns.getString("COLUMN_SIZE"));
            columnRequireds[i] = Integer.parseInt(columns.getString("NULLABLE"));
            columnAutoIncrementSigns[i] = columns.getString("IS_AUTOINCREMENT");
            columnRemarks[i] = columns.getString("REMARKS");
            columnDefaultValues[i] = columns.getString("COLUMN_DEF");
            i++;
        }
        Map<String, Object[]> tableParam = new HashMap<>(8);
        tableParam.put("columnNames", columnNames);
        tableParam.put("columnTypes", columnTypes);
        tableParam.put("columnSizes", columnSizes);
        tableParam.put("columnRequireds", columnRequireds);
        tableParam.put("columnAutoIncrementSigns", columnAutoIncrementSigns);
        tableParam.put("columnRemarks", columnRemarks);
        tableParam.put("columnDefaultValues", columnDefaultValues);
        tableParam.put("row", ArrayUtils.toArray(row));
        return tableParam;
    }

    private Map<String, List<IndexInfo>> obtainIndexParam(DatabaseMetaData metaData) throws SQLException {
        ResultSet indexInfo = metaData.getIndexInfo(null, null, tableName, false, false);
        Map<String, List<IndexInfo>> indexMap = new HashMap<>(8);
        while (indexInfo.next()) {
            boolean nonUnique = Boolean.parseBoolean(indexInfo.getString("NON_UNIQUE"));
            String indexName = indexInfo.getString("INDEX_NAME");
            String columnName = indexInfo.getString("COLUMN_NAME");
            Integer ordinalPosition = Integer.valueOf(indexInfo.getString("ORDINAL_POSITION"));
            IndexInfo info = new IndexInfo(nonUnique, indexName, columnName, ordinalPosition);
            if (indexMap.containsKey(indexName)) {
                indexMap.get(indexName).add(info);
            } else {
                List<IndexInfo> list = new ArrayList<>();
                list.add(info);
                indexMap.put(indexName, list);
            }
        }
        return indexMap;
    }

    private String parse(int row, Map<String, Object[]> tableParam, Map<String, List<IndexInfo>> indexMap, Set<String> pkColumn) {
        StringBuffer sb = new StringBuffer();
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        String date = sdf.format(new Date());

        sb.append("package " + this.scriptOutPath + "\r\n");
        sb.append("\r\n");
        sb.append("databaseChangeLog(logicalFilePath: \'" + this.tableName + ".groovy\') {\r\n");
        sb.append("\tchangeSet(author: \'" + this.scriptAuthor + "\', id: \'" + date + "-" + this.tableName + "\') {\r\n");
        sb.append("\t\tcreateTable(tableName: \"" + this.tableName + "\") {\r\n");
        for (int i = 0; i < row; i++) {
            String columnName = (String) tableParam.get("columnNames")[i];
            if (StringUtils.isEmpty(columnName)) {
                continue;
            }
            String columnType = (String) tableParam.get("columnTypes")[i];
            Integer columnSize = (Integer) tableParam.get("columnSizes")[i];
            if (columnType.toLowerCase().startsWith("timestamp")) {
                columnSize = 6;
            }

            Integer columnRequired = (Integer) tableParam.get("columnRequireds")[i];
            String columnAutoIncrementSign = (String) tableParam.get("columnAutoIncrementSigns")[i];
            String columnRemark = (String) tableParam.get("columnRemarks")[i];
            String columnDefaultValue = (String) tableParam.get("columnDefaultValues")[i];
            String autoIncrement = "YES".equalsIgnoreCase(columnAutoIncrementSign) ? "true" : "false";
            String nullable = columnRequired == null || columnRequired == 0 ? "false" : "true";
            String defaultValue = StringUtils.isEmpty(columnDefaultValue) ? "null" : columnDefaultValue;
            if (autoIncrement.equalsIgnoreCase("false")) {
                if (defaultValue.equalsIgnoreCase("null")) {
                    sb.append("\t\t\tcolumn(name: \'" + columnName + "\', type: \'" + columnType + "(" + columnSize + ")\', remarks: \"" + columnRemark + "\") {\r\n");
                } else {
                    if (defaultValue.equalsIgnoreCase("CURRENT_TIMESTAMP")) {
                        sb.append("\t\t\tcolumn(name: \'" + columnName + "\', type: \'" + columnType + "\',remarks:\"" + columnRemark + "\", defaultValueComputed: \"" + defaultValue + "\") {\r\n");
                    } else {
                        sb.append("\t\t\tcolumn(name: \'" + columnName + "\', type: \'" + columnType + "(" + columnSize + ")\', remarks: \"" + columnRemark + "\", defaultValue: \"" + defaultValue + "\") {\r\n");
                    }
                }
            } else {
                if (defaultValue.equalsIgnoreCase("null")) {
                    sb.append("\t\t\tcolumn(name: \'" + columnName + "\', type: \'" + columnType + "(" + columnSize + ")\', autoIncrement: " +
                            "" + autoIncrement + ", remarks: \"" + columnRemark + "\") {\r\n");
                } else {
                    if (defaultValue.equalsIgnoreCase("CURRENT_TIMESTAMP")) {
                        sb.append("\t\t\tcolumn(name: \'" + columnName + "\', type: \'" + columnType + "\', " + "autoIncrement: " +
                                "" + autoIncrement + ", remarks: \"" + columnRemark + "\", defaultValueComputed: \"" + defaultValue + "\") {\r\n");
                    } else {
                        sb.append("\t\t\tcolumn(name: \'" + columnName + "\', type: \'" + columnType + "(" + columnSize + ")\', autoIncrement: " +
                                "" + autoIncrement + ", remarks: \"" + columnRemark + "\", defaultValue: \"" + defaultValue + "\") {\r\n");
                    }
                }
            }

            if (autoIncrement.equalsIgnoreCase("true") || pkColumn.contains(columnName)) {
                sb.append("\t\t\t\tconstraints(nullable: " + nullable + ",\r\n");
                sb.append("\t\t\t\t\tprimaryKey: true\r\n");
                sb.append("\t\t\t\t)\r\n");
                sb.append("\t\t\t}\r\n");
            } else {
                sb.append("\t\t\t\tconstraints(nullable: " + nullable + ")\r\n");
                sb.append("\t\t\t}\r\n");
            }
        }
        sb.append("\t\t}\r\n");
        if (!CollectionUtils.isEmpty(indexMap)) {
            indexMap.forEach((indexName, indexInfoList) -> {
                if (!"PRIMARY".equalsIgnoreCase(indexName)) {
                    if (!indexInfoList.get(0).isNonUnique()) {
                        sb.append("\t\tcreateIndex(tableName: \"" + this.tableName + "\", indexName: \"" + indexName + "\", unique: " +
                                "true) {\r\n");
                    } else {
                        sb.append("\t\tcreateIndex(tableName: \"" + this.tableName + "\", indexName: \"" + indexName + "\", unique: " +
                                "false) {\r\n");
                    }
                    int j = 0;
                    while (j < indexInfoList.size()) {
                        for (int i = 0; i < indexInfoList.size(); i++) {
                            IndexInfo indexInfo = indexInfoList.get(i);
                            if (indexInfo.getOrdinalPosition() == j + 1) {
                                sb.append("\t\t\tcolumn(name: \"" + indexInfo.getColumnName() + "\")\r\n");
                                j++;
                            }
                        }
                    }
                    sb.append("\t\t}\r\n");
                }
            });
        }
        sb.append("\t}\r\n");
        sb.append("}");

        return sb.toString();
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }


    public void setScriptOutPath(String scriptOutPath) {
        this.scriptOutPath = scriptOutPath;
    }


    public void setScriptAuthor(String scriptAuthor) {
        this.scriptAuthor = scriptAuthor;
    }


    public void setUrl(String url) {
        this.url = url;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }


    public void setPassword(String password) {
        this.password = password;
    }


    public void setDriver(String driver) {
        this.driver = driver;
    }

    public void setTargetPath(String targetPath) {
        this.targetPath = targetPath;
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            System.out.println("\r\nconfigure the parameters for the jar package to run:\r\n" +
                    "1.[-target](required) : groovy script store path;\r\n" +
                    "2.[-url](required) : database connection path\r\n" +
                    "3.[-username](required) : database connection name\r\n" +
                    "4.[-password](required) : database connection password\r\n" +
                    "5.[-driver](required) : database driver\r\n" +
                    "6.[-author] : groovy script create person\r\n" +
                    "7.[-package] : groovy script package\r\n");
            return;
        }
        //脚本存放路径 需配置
        String targetPath = "";
        String url = "";
        String username = "";
        String password = "";
        String driver = "";
        String author = "";
        String pack = "";
        List<String> params = Arrays.asList(args);
        for (String param : params) {
            if (param.startsWith("-target")) {
                targetPath = param.substring(7);
            }
            if (param.startsWith("-url")) {
                url = param.substring(4);
            }
            if (param.startsWith("-driver")) {
                driver = param.substring(7);
            }
            if (param.startsWith("-username")) {
                username = param.substring(9);
            }
            if (param.startsWith("-password")) {
                password = param.substring(9);
            }
            if (param.startsWith("-author")) {
                author = param.substring(7);
            }
            if (param.startsWith("-package")) {
                pack = param.substring(8);
            }
        }
        if (StringUtils.isEmpty(targetPath)) {
            System.out.println("\r\nPlease configure the parameter groovy script store path [-target]");
            return;
        }
        if (StringUtils.isEmpty(url)) {
            System.out.println("\r\nPlease configure the database connection path [-url]");
            return;
        }
        if (StringUtils.isEmpty(username)) {
            System.out.println("\r\nPlease configure the database connection name [-username]");
            return;
        }
        if (StringUtils.isEmpty(password)) {
            System.out.println("\r\nPlease configure the database connection password [-password]");
            return;
        }
        if (StringUtils.isEmpty(driver)) {
            System.out.println("\r\nPlease configure the database driver [-driver]");
            return;
        }
        if (StringUtils.isEmpty(author)) {
            author = "default_author";
        }
        if (StringUtils.isEmpty(pack)) {
            pack = "default_package";
        }


        //获取微服务所有的数据库名称
        List<String> dbs = obtainAllDBName(url, username, password, driver);
//        dbs.removeAll(Arrays.asList("","","","sys"));
        if (CollectionUtils.isEmpty(dbs)) {
            return;
        }
        GroovyScriptGenerator generator;
        for (String db : dbs) {
            if ("information_schema".equalsIgnoreCase(db) ||
                    "mysql".equalsIgnoreCase(db) ||
                    "performance_schema".equals(db) ||
                    "sys".equalsIgnoreCase(db)) {
                continue;
            }
            //分别连接每个微服务库
            url = url.substring(0, url.indexOf("3306/") + 5) + db + url.substring(url.indexOf("?"));
            //初始化groovy生成器
            generator = initGroovyScriptGenerator(url, username, password, driver);
            //配置操作人
            generator.setScriptAuthor(author);
            //配置脚本包名
            generator.setScriptOutPath(pack);
            //配置每个微服务数据库groovy脚本存放的路径
            generator.setTargetPath(targetPath + db + "/");
            //分别获取每个微服务库的所有表名称
            List<String> tables = obtainAllTBName(url, username, password, db);
            for (String table : tables) {
                if (table.equalsIgnoreCase("databasechangelog") || table.equalsIgnoreCase("databasechangeloglock")) {
                    System.out.println("当前数据库为：" + db + "，当前表为：" + table + "跳过循环");
                    continue;
                }
                if (db.equalsIgnoreCase("workflow_service") && table.startsWith("act")) {
                    System.out.println("当前数据库为：" + db + "，当前表为：" + table + "跳过循环");
                    continue;
                }
                generator.setTableName(table);
                generator.generateGroovyScript(db);
            }
        }
    }

    private static List<String> obtainAllTBName(String url, String username, String password, String db) throws SQLException {
        String select_tb_sql = "select table_name from information_schema.tables where table_schema=?";
        Connection tbConnection = DriverManager.getConnection(url, username, password);
        PreparedStatement tbStatement = tbConnection.prepareStatement(select_tb_sql);
        tbStatement.setObject(1, db);
        ResultSet tbrs = tbStatement.executeQuery();
        List<String> tables = new ArrayList<>();
        while (tbrs.next()) {
            String tableName = tbrs.getString("table_name");
            tables.add(tableName);
        }
        tbrs.close();
        tbStatement.close();
        tbConnection.close();
        return tables;
    }

    private static GroovyScriptGenerator initGroovyScriptGenerator(String url, String username, String password, String driver) {
        GroovyScriptGenerator generator = new GroovyScriptGenerator();
        //配置数据库连接
        generator.setUrl(url);
        generator.setName(username);
        generator.setPassword(password);
        generator.setDriver(driver);
        return generator;
    }

    private static List<String> obtainAllDBName(String url, String username, String password, String driver) throws ClassNotFoundException, SQLException {
        String select_db_sql = "show databases";
        Class.forName(driver);
        Connection dbConnection = DriverManager.getConnection(url, username, password);
        PreparedStatement dbStatement = dbConnection.prepareStatement(select_db_sql);
        ResultSet dbrs = dbStatement.executeQuery();
        List<String> dbs = new ArrayList<>();
        while (dbrs.next()) {
            String dbName = dbrs.getString("Database");
            dbs.add(dbName);
        }
        dbrs.close();
        dbStatement.close();
        dbConnection.close();

        return dbs;
    }
}

class IndexInfo {
    /**
     * 是否唯一索引
     */
    private boolean nonUnique;
    /**
     * 索引名称
     */
    private String indexName;
    /**
     * 列名
     */
    private String columnName;
    /**
     * 组合索引中列的序列
     */
    private Integer ordinalPosition;

    public IndexInfo(boolean nonUnique, String indexName, String columnName, Integer ordinalPosition) {
        this.nonUnique = nonUnique;
        this.indexName = indexName;
        this.columnName = columnName;
        this.ordinalPosition = ordinalPosition;
    }

    public boolean isNonUnique() {
        return nonUnique;
    }

    public void setNonUnique(boolean nonUnique) {
        this.nonUnique = nonUnique;
    }

    public String getIndexName() {
        return indexName;
    }

    public void setIndexName(String indexName) {
        this.indexName = indexName;
    }

    public String getColumnName() {
        return columnName;
    }

    public void setColumnName(String columnName) {
        this.columnName = columnName;
    }

    public Integer getOrdinalPosition() {
        return ordinalPosition;
    }

    public void setOrdinalPosition(Integer ordinalPosition) {
        this.ordinalPosition = ordinalPosition;
    }
}
