#!/bin/bash
for number in {0..2} 
do
taosdemo -f /root/TDinternal/community/tests/pytest/perfbenchmark/benchmark_step/JSON/query_create_5_7_1.json &
done
wait