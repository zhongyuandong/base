-- 注册 kafka source 连接器， topic: szmsg_supply_label
-- create table default_catalog.default_database.source_connector_szmsg_supply_label (
--  `clientId` STRING,
--  `speedPre` BIGINT,
--  `msgId` STRING,
--  `source` STRING,
--  `deviceTime` BIGINT,
--  `speed` BIGINT,
--  `mcStatusPre` STRING,
--  `runCnt` BIGINT,
--  `mcStatus` STRING,
--  `runStartTime` BIGINT,
--  `dataDiff` STRING,
--  `timeDiff` BIGINT,
--  `lapCntPre` BIGINT,
--  `lapCnt` BIGINT,
--  `mcTime` BIGINT,
--  `cloudTime` BIGINT,
--  `factoryId` STRING,
--  `mechineId` STRING,
--  procTime AS TO_TIMESTAMP_LTZ(cloudTime, 0),
--  WATERMARK FOR procTime AS procTime - INTERVAL '60' SECOND
-- ) with (
--       'connector' = 'kafka',
--       'topic' = 'szmsg_supply_label',
--       'format' = 'json',
--       'properties.bootstrap.servers' = '192.168.22.140:9093,192.168.22.141:9093,192.168.22.142:9093',
--       'properties.group.id' = 'qb_szmsg_supply_label_cons_bigdata_toADB_test',
--       'properties.auto.offset.reset'='earliest',
--       'scan.startup.mode' = 'group-offsets',
--       'scan.topic-partition-discovery.interval' = '6s',
--       'json.ignore-parse-errors' = 'true'
--       );
--
-- SELECT *
-- FROM default_catalog.default_database.source_connector_szmsg_supply_label
-- WHERE clientId IS NOT NULL
--   AND deviceTime > 0
--   AND deviceTime <  UNIX_TIMESTAMP(CAST(TIMESTAMPADD(SECOND,1800,CURRENT_TIMESTAMP) AS STRING), 'yyyy-MM-dd HH:mm:ss');

-- 创建临时表用于窗口处理
-- CREATE TEMPORARY VIEW v_window_szmsg_supply_label AS
-- SELECT *
-- FROM default_catalog.default_database.source_connector_szmsg_supply_label
-- WHERE clientId IS NOT NULL
--   AND deviceTime > 0
--   AND deviceTime <  UNIX_TIMESTAMP(CAST(TIMESTAMPADD(SECOND,1800,CURRENT_TIMESTAMP) AS STRING), 'yyyy-MM-dd HH:mm:ss');
--
-- -- 按窗口泻入到ADB
-- select msgId,factoryId, mechineId,clientId,source,deviceTime,cloudTime,mcTime,timeDiff,lapCnt,lapCntPre,runCnt,runStartTime,mcStatus,mcStatusPre,speed,speedPre
-- FROM (
--          select msgId,factoryId, mechineId,clientId,source,deviceTime,cloudTime,mcTime,timeDiff,lapCnt,lapCntPre,runCnt,runStartTime,mcStatus,mcStatusPre,speed,speedPre,
--                 ROW_NUMBER() OVER(PARTITION BY window_start, window_end, clientId ORDER BY deviceTime DESC) rn
--          FROM TABLE(
--                  TUMBLE(TABLE v_window_szmsg_supply_label, DESCRIPTOR(procTime), INTERVAL '10' SECOND)
--              )
--      ) t
-- WHERE rn = 1
-- ;

create table default_catalog.default_database.source_connector_online_state_change_new (
  `deviceNum` STRING,
  `mcStatus` STRING,
  `factoryId` STRING,
  `mechineId` STRING,
  `cloudTime` BIGINT,
  procTime AS TO_TIMESTAMP_LTZ(cloudTime/1000, 0),
  WATERMARK FOR procTime AS procTime - INTERVAL '6' SECOND
) with (
      'connector' = 'kafka',
      'topic' = 'online_state_change_new',
      'format' = 'json',
      'properties.bootstrap.servers' = '192.168.22.140:9093,192.168.22.141:9093,192.168.22.142:9093',
      'properties.group.id' = 'qb_online_state_change_new_cons_bigdata_toADB_test',
      'properties.auto.offset.reset'='earliest',
      'scan.startup.mode' = 'group-offsets',
      'scan.topic-partition-discovery.interval' = '6s',
      'json.ignore-parse-errors' = 'true'
);

-- 创建临时表用于后续开窗处理
CREATE TEMPORARY VIEW v_window_online_state_change_new AS
SELECT *
FROM default_catalog.default_database.source_connector_online_state_change_new
WHERE deviceNum IS NOT NULL
  AND cloudTime > 0
  AND cloudTime <  UNIX_TIMESTAMP(CAST(TIMESTAMPADD(SECOND,1800,CURRENT_TIMESTAMP) AS STRING), 'yyyy-MM-dd HH:mm:ss')*1000;


--实时指标输出
SELECT
    machineId
    ,shiftId
    ,instId
    ,currentStatus
    ,FIRST_VALUE(cTime) OVER w AS startCtime
    ,LAST_VALUE(cTime) OVER w AS lastCtime
    ,LAST_VALUE(varietyId) OVER w AS varietyId
    ,LAST_VALUE(currentStrip) OVER w AS currentStrip
FROM
    default_catalog.default_database.warping_machine_source
window w AS (PARTITION BY machineId,shiftId,instId,currentStatus ORDER BY cDate ASC);

CREATE TEMPORARY VIEW v_tumble_window AS
SELECT
    *
FROM (
         SELECT
        window_start
         , window_end
         , cloth_code
         , MAX(edge_robot_weave_area)  AS edge_robot_weave_area
         , MAX(edge_robot_weave_loss_area) AS edge_robot_weave_loss_area
         , MAX(first_weave_area) AS first_weave_area
         , MAX(first_loss_area) AS first_loss_area
    FROM TABLE(
            TUMBLE(TABLE v_mid,DESCRIPTOR(row_time), INTERVAL '1' MINUTE)
        )
    GROUP BY window_start,window_end,GROUPING SETS((cloth_code))
) t WHERE cloth_code IS NOT NULL;


-- 按窗口大小写入数据到 ADB
-- insert into default_catalog.default_database.sink_connector_ods_woven_device_offline_status(factory_id,machine_id,device_num,online_status,online_time)
select factoryId, mechineId,deviceNum,mcStatus,cloudTime
FROM (
         select factoryId, mechineId,deviceNum,mcStatus, cloudTime,
                ROW_NUMBER() OVER(PARTITION BY window_start, window_end, deviceNum ORDER BY procTime DESC) rn
         FROM TABLE(
                 TUMBLE(TABLE v_window_online_state_change_new, DESCRIPTOR(procTime), INTERVAL '10' MINUTES)
             )
     ) t
WHERE rn = 1;