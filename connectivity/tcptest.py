#!/usr/bin/env python2
import sys
import socket
from multiprocessing import Pool
import datetime
# cqlsh -e "select peer from system.peers where data_center = 'dc1' ALLOW FILTERING;" | awk 'NR>3' | sed '$ d' | tr -d ' ' > peers.txt
file = sys.argv[1]
report = []
report2 = []
report3 = []

def separator(char, count):
    return char * count

def saveFile(path, data):
    with open(path, 'w') as f:
        f.write(data)
    return True

def addreport(*line):
    report.append(line)

def addreport2(*line):
    report2.append(line)

def printReport():
    for line in report:
        print(line)

def get_latency(i, count):
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(2)
    run = []
    for _ in range(count):
        start_time = datetime.datetime.now()
        try:
            sock.connect((i, 7000))
            end_time = datetime.datetime.now()
            sock.close()
            run.append((end_time - start_time).total_seconds() * 1000)
        except:
            run.append(9999)
    return run

def testLatency(records, interval, count):
    latency = {}
    addreport(separator('=', 10))
    addreport("Latency Test:")

    p = Pool(len(records))
    
    for i, latencies in p.map(get_latency, records)
        for a in i:
            report2[a] = latencies
            report3[a]['avg'] = round(sum(latencies) / len(latencies), 2)
            report3[a]['max'] = max(latencies)
            report3[a]['min'] = min(latencies)
            report3[a]['count'] = len(latencies)
    

    addreport(separator('=', 4))
    avglat = round(sum(latency.values()) / len(latency), 2)
    addreport("Average latency to contact points is", avglat,"ms")
    addreport2()
    return

addresses=[]
for line in open(file):
    addresses.append(line.strip())
    
testLatency(addresses, 1, 10)
#print(get_latency(addresses[0]))

printReport()