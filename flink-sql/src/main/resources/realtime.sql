create table default_catalog.default_database.warping_machine_source (
   mdata row(
           labels row(
             machineId varchar
           )
       ),
   sdata row(
       hcWarpYield row(
           cTime bigint,
           currentStatus bigint,
           currentStrip bigint,
           currentMeter bigint,
           endFlag bigint
       )
    )
    ,machineId as mdata.labels.machineId
    ,cTime as sdata.hcWarpYield.cTime
    ,cDate as TO_TIMESTAMP(FROM_UNIXTIME(cast(sdata.hcWarpYield.cTime as bigint), 'yyyy-MM-dd HH:mm:ss'))
    ,WATERMARK FOR cDate AS cDate - INTERVAL '3' SECOND
    ,currentStatus as sdata.hcWarpYield.currentStatus
    ,currentStrip as sdata.hcWarpYield.currentStrip
    ,currentMeter as sdata.hcWarpYield.currentMeter
    ,endFlag as sdata.hcWarpYield.endFlag
    ,preMeter bigint
    ,varietyId varchar
    ,shiftId varchar
    ,instId varchar
    ,row_time AS PROCTIME()
) with (
      'connector' = 'kafka',
      'topic' = 'flinksql_output_topic1120_2',
      'format' = 'json',
      'properties.bootstrap.servers' = 'qa-cdh-001:9093,qa-cdh-002:9093,qa-cdh-003:9093',
      'properties.group.id' = 'flinksql_topic_group1120_2',
      'scan.startup.mode' = 'group-offsets',
      'scan.topic-partition-discovery.interval' = '2s',
      'json.ignore-parse-errors' = 'true'
);

CREATE TEMPORARY VIEW mid_countMeter_view AS
SELECT
    t1.machineId
     ,t1.shiftId
     ,t1.instId
     ,t1.cDate
     ,t1.maxMeter
     ,COUNT(1) over (partition by t1.machineId,t1.shiftId,t1.instId,t1.maxMeter order by t1.cDate) as countMeter
FROM
    (
        SELECT
            machineId
             ,shiftId
             ,instId
             ,cDate
             ,MAX(currentMeter) over (partition by machineId,shiftId,instId,currentStrip order by cDate) as maxMeter
        FROM
            default_catalog.default_database.warping_machine_source
        WHERE currentStatus = 0
    ) t1;

SELECT
    t2.machineId
    ,t2.shiftId
    ,t2.instId
    ,t2.countMeter
    ,ROW_NUMBER() OVER(PARTITION BY t2.machineId,t2.shiftId,t2.instId ORDER BY t2.countMeter, t2.maxMeter) AS row_num
FROM mid_countMeter_view t2;
-- GROUP BY t2.machineId,t2.shiftId,t2.instId
-- ORDER BY t2.countMeter DESC, t2.maxMeter DESC;

-- CREATE TEMPORARY VIEW mid_batchMeter_maxMeter_view AS
-- SELECT
--     t2.machineId
--     ,t2.shiftId
--     ,t2.instId
--     ,t2.meterCount
--     ,t2.maxMeter
-- FROM
--     (
--         SELECT
--             t1.machineId
--              ,t1.shiftId
--              ,t1.instId
--              ,t1.maxMeter
--              ,t1.cDate
--              ,count(1 over (partition by t1.machineId, t1.shiftId, t1.instId, t1.maxMeter) as meterCount
-- --              ,procTime as proctime()
--         FROM
--             (
--                 SELECT
--                     machineId
--                      ,shiftId
--                      ,instId
--                      ,currentStrip
--                      ,cDate
--                      ,MAX(currentMeter) over (partition by machineId,shiftId,instId,currentStrip order by cDate) as maxMeter
--                 FROM
--                     default_catalog.default_database.warping_machine_source
--                 WHERE currentStatus = 0
--             ) t1
-- --         GROUP BY t1.machineId, t1.shiftId, t1.instId, t1.maxMeter
--     ) t2
-- where t2.maxMeter = MAX(t2.maxMeter) over (partition by t2.machineId, t2.shiftId, t2.instId order by t2.cDate);



--条数去重统计输出
-- SELECT
--     machineId
--      ,shiftId
--      ,instId
--      ,currentStatus
--      ,COUNT(distinct currentStrip) AS totalStrip
-- FROM
--     default_catalog.default_database.warping_machine_source
-- GROUP BY machineId ,shiftId ,instId ,currentStatus;

--实时指标输出
-- SELECT
--     machineId
--     ,shiftId
--     ,instId
--     ,currentStatus
--     ,FIRST_VALUE(cTime) OVER w AS startCtime
--     ,LAST_VALUE(cTime) OVER w AS lastCtime
--     ,LAST_VALUE(varietyId) OVER w AS varietyId
--     ,LAST_VALUE(currentStrip) OVER w AS currentStrip
-- FROM
--     default_catalog.default_database.warping_machine_source
-- window w AS (PARTITION BY machineId,shiftId,instId,currentStatus ORDER BY cDate ASC);