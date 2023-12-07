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

CREATE TEMPORARY VIEW mid_view AS
select
    t2.machineId
    ,t2.cDate
    ,t2.shiftId
    ,t2.instId
    ,t2.currentStatus
    ,t2.currentStrip
    ,t2.currentMeter
    ,t2.preMeter
    ,t2.preStrip
    ,t2.preShiftId
    ,t2.preInstId
    ,case
        when t2.instId = 'null' OR t2.preShiftId = 'null'
    then (case when t2.currentStatus = 0 then 0 else t2.currentMeter end)
        when t2.currentStrip = t2.preStrip and (t2.shiftId <> t2.preShiftId OR t2.instId <> t2.preInstId)
    then t2.preMeter
        when t2.shiftId = t2.preShiftId AND t2.instId = t2.preInstId AND t2.currentStrip <> t2.preStrip
    then 0
        else t2.preMeter end batcnMeterDiff
from (
        SELECT
            t1.machineId
            ,t1.shiftId
            ,t1.instId
            ,t1.currentStatus
            ,t1.currentStrip
            ,t1.currentMeter
            ,t1.cDate
            ,LAG(t1.currentMeter, 1, cast(-1 as bigint)) OVER w AS preMeter
            ,LAG(t1.currentStrip, 1, cast(-1 as bigint)) OVER w AS preStrip
            ,LAG(t1.shiftId, 1, 'null') OVER w AS preShiftId
            ,LAG(t1.instId, 1, 'null') OVER w AS preInstId
        FROM
            default_catalog.default_database.warping_machine_source t1
        window w AS (PARTITION BY t1.machineId ORDER BY t1.cDate)
    ) t2;


CREATE TEMPORARY VIEW mid_batchMeter_zj_view AS
select
    *
from
    (
        SELECT
            window_start as startTime
             ,window_end
             ,machineId
             ,shiftId
             ,instId
             ,currentStrip
             ,currentStatus
             ,MAX(currentMeter) - MIN(batcnMeterDiff)  AS bachMeter
        FROM TABLE(
                TUMBLE(TABLE mid_view,DESCRIPTOR(cDate), INTERVAL '60' SECOND)
            )
        GROUP BY window_start,window_end,GROUPING SETS((machineId,currentStrip,shiftId,instId,currentStatus))
    ) t where currentStatus = 0;

-- 整经产量
select
    machineId
    ,shiftId
    ,instId
    ,SUM(bachMeter) as meter
FROM mid_batchMeter_zj_view
GROUP BY machineId,shiftId,instId;


CREATE TEMPORARY VIEW mid_batchMeter_dz_view AS
select
    *
from
    (
        SELECT
            window_start as startTime
             ,window_end
             ,machineId
             ,shiftId
             ,instId
             ,currentStrip
             ,currentStatus
             ,MAX(batcnMeterDiff) - MIN(currentMeter)  AS bachMeter
        FROM TABLE(
                TUMBLE(TABLE mid_view,DESCRIPTOR(cDate), INTERVAL '60' SECOND)
            )
        GROUP BY window_start,window_end,GROUPING SETS((machineId,currentStrip,shiftId,instId,currentStatus))
    ) t where currentStatus = 1;

-- 倒轴产量
select
    machineId
     ,shiftId
     ,instId
     ,SUM(bachMeter) as meter
FROM mid_batchMeter_dz_view
GROUP BY machineId,shiftId,instId;