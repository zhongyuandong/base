create table warping_machine_source (
    cycle1 bigint
    ,machineId varchar
    ,cTime bigint
    ,curDate as TO_TIMESTAMP(FROM_UNIXTIME(cTime, 'yyyy-MM-dd HH:mm:ss'))
    ,WATERMARK FOR curDate AS curDate - INTERVAL 5 SECOND
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
      'scan.topic-partition-discovery.interval' = '6s',
      'json.ignore-parse-errors' = 'true'
);



create table sink_print(
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

insert into sink_print
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
    warping_machine_source;

