create table default_catalog.default_database.warping_machine_source (
    cycle1 bigint
    ,machineId varchar
    ,cTime bigint
--     ,curDate as TO_TIMESTAMP(FROM_UNIXTIME(cTime, 'yyyy-MM-dd HH:mm:ss'))
--     ,curDate as CAST(TO_TIMESTAMP(FROM_UNIXTIME(cTime, 'yyyy-MM-dd HH:mm:ss')) as TIME)
    ,curDate as TO_TIMESTAMP(FROM_UNIXTIME(cTime, 'yyyy-MM-dd HH:mm:ss'))
    ,WATERMARK FOR curDate AS curDate - INTERVAL '3' SECOND
    ,shiftId varchar
    ,instId varchar
    ,row_time AS PROCTIME()
) with (
      'connector' = 'kafka',
      'topic' = 'warping_machine_topic_test3',
      'format' = 'json',
      'properties.bootstrap.servers' = 'qa-cdh-001:9093,qa-cdh-002:9093,qa-cdh-003:9093',
      'properties.group.id' = 'warping_machine_topic_test_group',
      'scan.startup.mode' = 'group-offsets',
      'scan.topic-partition-discovery.interval' = '600s',
      'json.ignore-parse-errors' = 'true'
);



create table default_catalog.default_database.sink_print(
    cycle1 bigint
    ,machineId varchar
    ,cTime bigint
    ,curDate TIME(3)
    ,shiftId varchar
    ,instId varchar
    ,row_time TIMESTAMP_LTZ(3)
    ,firstCycle bigint
    ,maxTime bigint
)
    with (
        'connector'='print'
        );

insert into default_catalog.default_database.sink_print
select
    cycle1
     ,machineId
     ,cTime
     ,curDate
     ,shiftId
     ,instId
     ,row_time
     ,FIRST_VALUE(cycle1) over (partition by machineId order by curDate asc) as firstCycle
    ,Max(cTime) over (partition by machineId order by curDate asc) as maxTime
from
    default_catalog.default_database.warping_machine_source;

-- CREATE TEMPORARY VIEW mid_view AS
-- SELECT
--     factory_id
--      , cloth_code
--      , edge_robot_weave_area
--      , edge_robot_weave_loss_area
--      , device_time
--      , row_time
--      , first_weave_area
--      , first_loss_area
-- FROM (
--          SELECT
--              factory_id
--               , cloth_code
--               , edge_robot_weave_area
--               , edge_robot_weave_loss_area
--               , device_time
--               , row_time
--               , FIRST_VALUE(edge_robot_weave_area) over (partition by cloth_code order by row_time asc) AS first_weave_area
--     , FIRST_VALUE(edge_robot_weave_loss_area) over (partition by cloth_code order by row_time asc) AS first_loss_area
--     , MAX(device_time) over (partition by cloth_code order by row_time asc) AS max_device_time
--          FROM default_catalog.default_database.source_connector_gemi_device_upstream_qms
--      ) t
-- WHERE t.device_time >= max_device_time
;


