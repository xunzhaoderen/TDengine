#!/bin/bash

var=100;
$add=ecs1
var2=0

echo "$var2"

ssh root@20.98.75.200 << eeooff
var2=echo "$var"
echo "ecs1"
echo "$var2"
exit
eeooff

echo "$var2"
