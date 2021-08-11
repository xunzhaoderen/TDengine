#!/bin/bash

tableNum=10000000
standardTimeout=600
addr="20.98.75.200"
addr2="20.98.76.209"
clientAddr="ecs4"
fileName="data"

while getopts "n:a:b:c:f:" opt; do
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
    f)
        fileName=$OPTARG
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
        echo "======================================================"
        exit 1
        ;;
    esac
done

#stopping taosd and clean the files
#also set up the cluster
ssh root@$addr <<eeooff
    systemctl stop taosd
    rm -rf /data/taos/data/*
    systemctl start taosd
    exit
eeooff
ssh root@$addr2 <<eeooff
    systemctl stop taosd
    rm -rf /data/taos/data/*
    systemctl start taosd
    sleep 20
    taos -s "create dnode \'ecs2:6030\'"
    taos -s "create database db"
    exit
eeooff

## setup the json files
cd ../..
echo `python3 test.py -f perfbenchmark/benchmark_step/file_generation.py 1>/dev/null`
cd perfbenchmark/benchmark_step
scp -r /root/TDinternal/community/tests/pytest/perfbenchmark/benchmark_step/ $clientAddr:/root/TDinternal/community/tests/pytest/perfbenchmark/
echo "scp -r /root/TDinternal/community/tests/pytest/perfbenchmark/benchmark_step/ $clientAddr:/root/TDinternal/community/tests/pytest/perfbenchmark/"
echo "strat to create tables"
echo `date`
python3 main.py -t create -a $addr -b $clientAddr -n $tableNum -f create_$tableNum.log
echo "table creation finished"
echo `date`
sleep 60


##free the buffer during table creation
echo ''
ssh root@$addr2 <<eeooff
    systemctl restart taosd
    exit
eeooff
ssh root@$addr <<eeooff
    systemctl restart taosd
    exit
eeooff
sleep $standardTimeout
echo "taosd restarted"
echo `date`
echo ''

echo "start to insert data"
echo `date`
python3 main.py -t insertParallel -a $addr -b $clientAddr -n $tableNum -f insert_$tableNum.log 
echo "table insert finished"
echo `date`

##close taosd to force all data to disk
echo ''
ssh root@$addr2 <<eeooff
    systemctl stop taosd
    exit
eeooff
ssh root@$addr <<eeooff
    systemctl stop taosd
    exit
eeooff
sleep $standardTimeout

echo "the following are the vnode size for the two servers"
echo `ssh root@$addr du -sh /data/taos/${fileName}/vnode | cut -d '	' -f 1 `
echo `ssh root@$addr2 du -sh /data/taos/${fileName}/vnode | cut -d '	' -f 1 `

echo "the following are the mnode file size for the two servers"
echo `ssh root@$addr du -sh /data/taos/${fileName}/mnode | cut -d '	' -f 1 `
echo `ssh root@$addr2 du -sh /data/taos/${fileName}/mnode | cut -d '	' -f 1 `

ssh root@$addr2 <<eeooff
    systemctl start taosd
    exit
eeooff
ssh root@$addr <<eeooff
    systemctl start taosd
    exit
eeooff
sleep $standardTimeout
echo "taosd restarted"
echo `date`
echo ''


echo "start to run single queries"
echo `date`
python3 multiIndividualQuery.py 1>/dev/null
echo "single queries finished"
echo `date`
echo ''

echo "start to run concurrent query for last_row() 100"
echo `date`
echo `taosdemo -f JSON/query_create_2_1.json 1>/dev/null`
echo "last_row() concurrent finished"
echo `date`
echo ''

echo "start to run concurrent query for last_row() 500 times"
echo `date`
echo `sed -i "s/for number in.*/for number in {0..5} /g" JSON/go.sh`
echo `sed -i "s/*query_create_5_7_1.json*/query_create_2_1.json /g" JSON/go.sh`
echo `JSON/go.sh 1>/dev/null`
echo "last_row() concurrent finished"
echo `date`
echo ''

echo "start to run concurrent query for last_row() 1000 times"
echo `date`
echo `sed -i "s/for number in.*/for number in {0..10} /g" JSON/go.sh`
echo `JSON/go.sh 1>/dev/null`
echo "last_row() concurrent finished"
echo `date`
echo ''


echo "start to run concurrent query for last_row() 5000 times"
echo `date`
echo `sed -i "s/for number in.*/for number in {0..50} /g" JSON/go.sh`
echo `JSON/go.sh 1>/dev/null`
echo "last_row() concurrent finished"
echo `date`
echo ''

echo "start to run concurrent query for last_row() 100 times"
echo `date`
echo `sed -i "s/for number in.*/for number in {0..1} /g" JSON/go.sh`
echo `sed -i "s/*query_create_2_1.json*/query_create_5_7_1.json /g" JSON/go.sh`
echo `JSON/go.sh 1>/dev/null`
echo "last_row() concurrent finished"
echo `date`
echo ''

echo "start to run concurrent query for last_row() 200 times"
echo `date`
echo `sed -i "s/for number in.*/for number in {0..2} /g" JSON/go.sh`
echo `JSON/go.sh 1>/dev/null`
echo "last_row() concurrent finished"
echo `date`
echo ''

echo "start to run concurrent query for last_row() 300 times"
echo `date`
echo `sed -i "s/for number in.*/for number in {0..3} /g" JSON/go.sh`
echo `JSON/go.sh 1>/dev/null`
echo "last_row() concurrent finished"
echo `date`
echo ''

echo "start to run concurrent query for last_row() 400 times"
echo `date`
echo `sed -i "s/for number in.*/for number in {0..4} /g" JSON/go.sh`
echo `JSON/go.sh 1>/dev/null`
echo "last_row() concurrent finished"
echo `date`
echo ''


# echo "start to run continous query"
# nohup python3 main.py -t insertParallel -a $addr -b $clientAddr -n $tableNum -f insert_$tableNum.log >>/dev/null &
# echo "insert started start query"
# echo `date`
# echo `python3 main.py -t contious_query`
# echo `date`
# echo ''
# echo `pkill taosdemo`
# echo `ssh root@$clientAddr pkill taosdemo`

# ssh root@$addr2 <<eeooff
#     systemctl stop taosd
#     exit
# eeooff

# ssh root@$addr <<eeooff
#     systemctl stop taosd
#     exit
# eeooff

rm JSON/*.json