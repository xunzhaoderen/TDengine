#!/bin/bash

tableNum=100000000
addr="20.98.75.200"
addr2="20.98.76.209"
clientAddr="ecs4"
queryRun='none'

while getopts "n:a:b:c:q:" opt; do
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

ssh root@$addr2 <<eeooff
    systemctl start taosd
    exit
eeooff
systemctl start taosd
sleep (3600)