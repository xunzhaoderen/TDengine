#!/bin/bash
for number in {0..4} 
do
taosdemo -f /root/TDinternal/community/tests/pytest/perfbenchmark/benchmark_step/JSON/query_create_2_1.json &
done
wait