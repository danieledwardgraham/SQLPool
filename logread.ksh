while true
do
    echo "--------------BEGIN OF LOG-----------" >> serverLog.txt
    echo "--------------BEGIN OF LOG-----------" >> clientLog.txt
    readlog 10000 >> serverLog.txt 2>&1
    readlogcli 10000 >> clientLog.txt 2>&1
    sleep 5
done
