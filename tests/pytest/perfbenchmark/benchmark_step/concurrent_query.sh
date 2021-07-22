#!/bin/bash

tableNum=10000000
standardTimeout=600
addr="20.98.75.200"
addr2="20.98.76.209"
clientAddr="ecs4"
queryRun='none'

while getopts "n:a:b:c:q:" opt; do
    case $opt in
    n)
        tableNum=$OPTARG
        standardTimeout=$((600*$tableNum/10000000))
        echo "$standardTimeout"
        ;;
    a)
        addr=$OPTARG
        ;;
    b)
        clientAddr=$OPTARG
        ;;
    c)
        addr2=$OPTARG
        ;;
    q)
        queryRun=$OPTARG
        ;;
    ?)
        echo "======================================================"
        echo "n | number of tables"
        echo "------------------------------------------------------"
        echo "a | address of the server with first FQDN"
        echo "------------------------------------------------------"
        echo "b | the other client responsible for insert"
        echo "------------------------------------------------------"
        echo "c | the other server that also runs TDengine"
        echo "------------------------------------------------------"
        echo "q | the query that is going to run"
        echo "======================================================"
        exit 1
        ;;
    esac
done

cd ../..
echo `python3 test.py -f perfbenchmark/benchmark_step/test.py 1>/dev/null`
cd perfbenchmark/benchmark_step

ssh root@$addr2 <<eeooff
    systemctl restart taosd
    exit
eeooff
ssh root@$addr <<eeooff
    systemctl restart taosd
    exit
eeooff
systemctl start taosd
sleep $standardTimeout

echo "start to run concurrent query for select sum(col1), avg(col2) from stb for 50 times"
echo `date`
echo `sed -i "s/for number in.*/for number in {0..1} /g" JSON/go.sh`
echo `JSON/go.sh 1>/dev/null`
echo "last_row() concurrent finished"
echo `date`
echo ''

rm JSON/*.json