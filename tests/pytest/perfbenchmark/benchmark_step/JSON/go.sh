#!/bin/bash
for number in {0..10}
do
taosdemo -f query_create_2.json &
done
wait