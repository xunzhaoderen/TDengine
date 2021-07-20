!/bin/bash

tableNum=100000000
addr="20.98.75.200"
addr2="20.98.76.209"
clientAddr="ecs4"

while getopts "n:a:b:c:" opt; do
    case $opt in
    n)
        tableNum=$OPTARG
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

##stopping taosd and clean the files
ssh root@addr <<eeooff
    systemctl stop taosd
    rm -rf /data/taos/data/*
    systemctl start taosd
    exit
eeooff
ssh root@$addr2 <<eeooff
    systemctl stop taosd
    rm -rf /data/taos/data/*
    systemctl start taosd
    exit
eeooff
sleep(60)
##

## setup the cluster
echo `taos -h $add -s "create dnode 'ecs3:6030'"`
echo `taos -h $add -s "create database db"`

## setup the json files
cd ../..
echo `python3 test.py -f perfbenchmark/benchmark_step/test.py 1>/dev/null`
cd perfbenchmark/benchmark_step
echo `scp -r /root/TDinternal/community/tests/pytest/perfbenchmark/benchmark_step/ $addr:/root/TDinternal/community/tests/pytest/perfbenchmark/ 1>/dev/null`

echo "strat to create tables"
echo `date`
echo `python3 main.py -t create -a $addr -b $clientAddr -n $tableNum create_$tableNum.log 1>/dev/null`
echo "table creation finished"
echo `date`


##free the buffer during table creation
echo ''
ssh root@$addr2 <<eeooff
    systemctl stop taosd
    exit
eeooff
ssh root@$addr <<eeooff
    systemctl stop taosd
    exit
eeooff
sleep (1800)

ssh root@$addr2 <<eeooff
    systemctl start taosd
    exit
eeooff
ssh root@$addr <<eeooff
    systemctl start taosd
    exit
eeooff
sleep (3600)
echo "taosd restarted"
echo "date"
echo ''

echo "start to insert data"
echo `date`
echo `python3 main.py -t insertParallel -a $addr -b $clientAddr -n $tableNum -f insert_$tableNum.log 1>/dev/null`
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
sleep (1800)

echo "the following are the vnode size for the two servers"
echo `ssh root@$add du -sh /data/taos/data/vnode | cut -d '	' -f 1 `
echo `ssh root@$add2 du -sh /data/taos/data/vnode | cut -d '	' -f 1 `

echo "the following are the mnode file size for the two servers"
echo `ssh root@$add du -sh /data/taos/data/mnode | cut -d '	' -f 1 `
echo `ssh root@$add2 du -sh /data/taos/data/mnode | cut -d '	' -f 1 `

ssh root@$addr2 <<eeooff
    systemctl start taosd
    exit
eeooff
ssh root@$addr <<eeooff
    systemctl start taosd
    exit
eeooff
sleep (3600)
echo "taosd restarted"
echo "date"
echo ''


echo "start to run single queries"
echo `date`
python3 multiIndividualQuery.py 1>/dev/null
echo "single queries finished"
echo `date`

echo "start to run concurrent query for last_row()"
echo `date`
taosdemo -f JSON/query_create_2_1.json 1>dev/null
echo "last_row() concurrent finished"
echo `date`

echo ''

ssh root@$addr2 <<eeooff
    systemctl stop taosd
    exit
eeooff
systemctl stop taosd

rm JSON/*.json