create table default_catalog.default_database.warping_machine_source (
                                                                         mdata row(
                                                                             device row(
                                                                             name varchar
                                                                             ),
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
    ,deviceNum as mdata.device.name
    ,machineId as mdata.labels.machineId
    ,cTime as sdata.hcWarpYield.cTime
    ,currentStatus as sdata.hcWarpYield.currentStatus
    ,currentStrip as sdata.hcWarpYield.currentStrip
    ,currentMeter as sdata.hcWarpYield.currentMeter
    ,endFlag as sdata.hcWarpYield.endFlag
    ,shiftId varchar
    ,instId varchar
    ,row_time AS PROCTIME()
) with (
      'connector' = 'kafka',
      'topic' = 'warping_machine_topic_test',
      'format' = 'json',
      'properties.bootstrap.servers' = 'qa-cdh-001:9093,qa-cdh-002:9093,qa-cdh-003:9093',
      'properties.group.id' = 'warping_machine_topic_test_group',
      'scan.startup.mode' = 'group-offsets',
      'scan.topic-partition-discovery.interval' = '600s',
      'json.ignore-parse-errors' = 'true'
      );


create table default_catalog.default_database.sink_print(
    machineId varchar
    ,shiftId varchar
    ,instId varchar
    ,currentStatus bigint
    ,currentStrip bigint
    ,currentMeter bigint
    ,preShiftId varchar
    ,preInstId varchar
)
    with (
        'connector'='print'
        );

insert into default_catalog.default_database.sink_print
SELECT
    t1.machineId,
    t1.shiftId,
    t1.instId,
    t1.currentStatus,
    t1.currentStrip,
    t1.currentMeter
        ,lead( t1.shiftId ) over ( PARTITION BY t1.machineId ORDER BY t1.row_time ASC ) AS preShiftId
        ,lead( t1.instId ) over ( PARTITION BY t1.machineId ORDER BY t1.row_time ASC ) AS preInstId
FROM
    default_catalog.default_database.warping_machine_source t1;