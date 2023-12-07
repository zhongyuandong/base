package com.zyd.tech.debugentry.constance;

/**
 * @program: base
 * @description:
 * @author: zhongyuandong
 * @create: 2023-08-10 15:08:02
 * @Version 1.0
 **/
public interface Constants {

    public static final String SORT_PROPERTY_UPDATE_TIME = "UPDATE_TIME";
    public static final String SORT_DIRECTION_ASC = "ASC";
    public static final String SORT_DIRECTION_DESC = "DESC";

    public static final int NUMBER_ZERO = 0;
    public static final int NUMBER_ONE = 1;
    public static final String STRING_NEGATIVE_ONE = "-1";
    public static final String STRING_ONE = "1";
    public static final String STRING_TWO = "2";
    public static final int NUMBER_THREE = 3;
    public static final int NUMBER_TWO = 2;
    public static final int NUMBER_FOUR = 4;
    public static final int NUMBER_TWO_HUNDRED_AND_FIFTY_FIVE = 255;
    public static final int NUMBER_FIVE_HUNDRED_AND_TWELVE = 512;

    public static final String STRING_ZERO = "0";

    public static final String STRING_G = "G";

    public static final String STRING_NULL = "";
    public static final String DOT = ".";
    public static final int FIELD_NED_NAME_MAX = 50;
    public static final int FIELD_NAME_MAX = 200;
    /**
     * 大小写字母+数字+下划线
     */
    public static final String REG_LETTER_NUMBER_UNDERLINE = "^\\w+$";
    /**
     * 大小写字母+数字+下划线 字母开头
     */
    public static final String REG_LETTER_NUMBER_UNDERLINE_BEGINNING_OF_LETTER = "^[a-zA-Z][a-zA-Z0-9_]+$";

    /**
     * 中文+大小写字母+数字+下划线的组合 字母中文开头 ^[\u4E00-\u9FA5a-zA-Z][\u4E00-\u9FA5a-zA-Z0-9_]*$
     */
    public static final String REG_CHINESE_LETTER_NUMBER_UNDERLINE_BEGINNING_OF_LETTER =  "^[\\u4E00-\\u9FA5a-zA-Z][\\u4E00-\\u9FA5a-zA-Z0-9_]*$";
    /**
     * 中英文
     */
    public static final String REG_CHINESE_LETTER = "^[a-zA-Z\\u4E00-\\u9FA5]+$";

    /**
     * 中文+大小写字母+数字+下划线的组合
     */
    public static final String REG_CHINESE_LETTER_NUMBER_UNDERLINE = "^[\\u4e00-\\u9fa5\\w]+$";

    /**
     * 中文+大小写字母+数字+下划线+中划线+点的组合
     */
    public static final String REG_CHINESE_LETTER_NUMBER_UNDERLINE_STRIKETHROUGH_POINT = "^[\\u4e00-\\u9fa5\\w-.]+$";

    /**
     * 大小写字母+数字+下划线+中划线+点的组合
     */
    public static final String REG_LETTER_NUMBER_UNDERLINE_STRIKETHROUGH_POINT = "^[\\w-.]+$";

    /**
     * 忽略大小写和前后空格匹配desc/asc的正则表达式
     */
    public static final String REG_ASC_OR_DESC = "^\\s*(?i)(ASC|DESC)\\s*$";

    /**
     * 不包含英文逗号的正则表达式
     */
    public static final String REG_EXCLUDE_EN_COMMA = "^(?!.*?,).*$";


    /**
     * 小写字母和冒号
     */
    public static final String REG_ENGLISH_OR_COLON = "^[a-z:]+$";

    /**
     * 反斜杠
     */
    public static final String BACKSLASH = "/";

    /**
     * 最新的checkpoint
     */
    public final static String CHECKPOINT_LATEST = "THE_LATEST";


    public static final String STRING_INSERT = "insert ";

    public static final String STRING_SET = "set ";

    public static final String STRING_CREATE = "create ";

    public static final String STRING_DELETE = "delete ";

    public static final String STRING_UPDATE = "update ";

    public static final String STRING_SYSTEM = "system";

    public static final String STRING_POINT = "...";

    public static final String STRING_STATUS = "status";


    /**
     * date format of yyyyMMddHHmmss
     */
    public static final String NORMAL_DATETIME_FORMAT = "yyyy-MM-dd HH:mm:ss";
    public static final String YYYYMMDD = "yyyyMMdd";
    public static final String YYYYMMDD_WITH_DASH = "yyyy-MM-dd";
    public static final String YYYYMMDDHHMMSS = "yyyyMMddHHmmss";

    /**
     * jdbc url
     */
    public static final String MONGODB = "mongodb://";
    public static final String JDBC_POSTGRESQL = "jdbc:postgresql://";
    public static final String JDBC_CLICKHOUSE = "jdbc:clickhouse://";
    public static final String JDBC_ORACLE_SID = "jdbc:oracle:thin:@";
    public static final String JDBC_ORACLE_SERVICE_NAME = "jdbc:oracle:thin:@//";
    public static final String JDBC_SQLSERVER = "jdbc:sqlserver://";
    public static final String JDBC_DB2 = "jdbc:db2://";

    /**
     * database connection driver
     */
    public static final String ORG_POSTGRESQL_DRIVER = "org.postgresql.Driver";
    public static final String COM_CLICKHOUSE_JDBC_DRIVER = "ru.yandex.clickhouse.ClickHouseDriver";
    public static final String COM_ORACLE_JDBC_DRIVER = "oracle.jdbc.driver.OracleDriver";
    public static final String COM_SQLSERVER_JDBC_DRIVER = "com.microsoft.sqlserver.jdbc.SQLServerDriver";
    public static final String COM_DB2_JDBC_DRIVER = "com.ibm.db2.jcc.DB2Driver";


    /**
     * SQL CATALOG默认版本
     */
    public static final String SQL_CATALOG_DEFAULT_VERSION = "2.1.1";

    /**
     * 最新的checkpoint
     */
    public final static String ACCESS_TOKEN = "accessToken";


    public static final String ALL = "__ALL__";

    /**
     * 规则 root id
     */
    public static final String RULE_ROOT_ID = "0";


    /**
     * date format of yyyy-MM-dd HH:mm:ss
     */
    public static final String YYYY_MM_DD_HH_MM_SS = "yyyy-MM-dd HH:mm:ss";

    /**
     * date format of yyyyMMddHHmmssSSS
     */
    public static final String YYYYMMDDHHMMSSSSS = "yyyyMMddHHmmssSSS";
    /**
     * comma ,
     */
    public static final String COMMA = ",";

    /**
     * slash /
     */
    public static final String SLASH = "/";

    /**
     * COLON :
     */
    public static final String COLON = ":";

    /**
     * SPACE " "
     */
    public static final String SPACE = " ";

    /**
     * SINGLE_SLASH /
     */
    public static final String SINGLE_SLASH = "/";

    /**
     * DOUBLE_SLASH //
     */
    public static final String DOUBLE_SLASH = "//";

    /**
     * SINGLE_QUOTES "'"
     */
    public static final String SINGLE_QUOTES = "'";
    /**
     * DOUBLE_QUOTES "\""
     */
    public static final String DOUBLE_QUOTES = "\"";

    /**
     * SEMICOLON ;
     */
    public static final String SEMICOLON = ";";

    /**
     * EQUAL SIGN
     */
    public static final String EQUAL_SIGN = "=";
    /**
     * AT SIGN
     */
    public static final String AT_SIGN = "@";
    /**
     * date format of yyyyMMdd
     */
    public static final String PARAMETER_FORMAT_DATE = "yyyyMMdd";

    /**
     * date format of yyyyMMddHHmmss
     */
    public static final String PARAMETER_FORMAT_TIME = "yyyyMMddHHmmss";
    /**
     * month_begin
     */
    public static final String MONTH_BEGIN = "month_begin";
    /**
     * add_months
     */
    public static final String ADD_MONTHS = "add_months";
    /**
     * month_end
     */
    public static final String MONTH_END = "month_end";
    /**
     * week_begin
     */
    public static final String WEEK_BEGIN = "week_begin";
    /**
     * week_end
     */
    public static final String WEEK_END = "week_end";
    /**
     * timestamp
     */
    public static final String TIMESTAMP = "timestamp";
    public static final char SUBTRACT_CHAR = '-';
    public static final char ADD_CHAR = '+';
    public static final char MULTIPLY_CHAR = '*';
    public static final char DIVISION_CHAR = '/';
    public static final char LEFT_BRACE_CHAR = '(';
    public static final char RIGHT_BRACE_CHAR = ')';
    public static final String ADD_STRING = "+";
    public static final String MULTIPLY_STRING = "*";
    public static final String DIVISION_STRING = "/";
    public static final String LEFT_BRACE_STRING = "(";
    public static final char P = 'P';
    public static final char N = 'N';
    public static final String SUBTRACT_STRING = "-";

    public static final String AUTHORIZATION = "Authorization";
    public static final String BEARER_SPACE = "Bearer ";

}
