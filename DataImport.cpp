#include <assert.h>
#include <math.h>
#include <algorithm>
#include <iostream>
#include "Cluster.h"
#include "DataImport.h"
using namespace HadoopNetSim;
using namespace std;

/* DataImport Local Variables */
static unsigned long rackIndex = 0;
static unordered_map<string, unsigned long> nodeIndexPerRack;
static map<string, long> nodeBlockCnt;
static long importInterval;
static long importBlocks; // num of blocks within the file to be transferred
static long leftBlocks;
static long blockStep = 10;
static long blockSize;
static long blockPerTT = 0;
static int importType;
static unordered_map<MsgId, vector<HostName>> sentMsg;

enum ImportType {
    RepeatedImport,
    RandomImport,
    IncrementalImport,
    DecrementalImport
};

vector<HostName> findBlockDestination(void)
{
    unordered_map<RackName, vector<HostName>> rackSet = getRackSet();
    size_t rackNum = rackSet.size();
    vector<HostName> dataNodes;

    while(1) {
        string rackName = "rack" + to_string(rackIndex % rackNum);
        vector<HostName> tt = rackSet[rackName];
        assert(!tt.empty());
        size_t i, j, k;
        for(k = 0, i = nodeIndexPerRack[rackName]; k < tt.size(); k++, i++) {
            if(tt[i%tt.size()] == "manager") continue;
            if(nodeBlockCnt[tt[i%tt.size()]] < blockPerTT)
                break;
        }
        if (k == tt.size()) {
            rackIndex++;    // all tt(s) in current rack are full, try next rack
            continue;
        }
        nodeIndexPerRack[rackName]++;

        if(dataNodes.size() == 2) {
            dataNodes.push_back(tt[i%tt.size()]);
            nodeBlockCnt[tt[i%tt.size()]]++;
            break;
        }

        for(k = 0, j = nodeIndexPerRack[rackName]; k < tt.size(); k++, j++) {
            if (i == j) continue;
            if(tt[j%tt.size()] == "manager") continue;
            if(nodeBlockCnt[tt[j%tt.size()]] < blockPerTT)
                break;
        }
        if (k == tt.size()) {
            rackIndex++;
            continue;
        }
        nodeIndexPerRack[rackName]++;

        dataNodes.push_back(tt[i%tt.size()]); nodeBlockCnt[tt[i%tt.size()]]++;
        dataNodes.push_back(tt[j%tt.size()]); nodeBlockCnt[tt[j%tt.size()]]++;
        rackIndex++;
    }
    assert(dataNodes.size() == 3);
    rackIndex++;
    return dataNodes;
}

void ImportRequest(long blocks);
void ImportFinish(ns3::Ptr<MsgInfo> response_msg)
{
    vector<HostName> pipeline = sentMsg.at(response_msg->id());
    assert(pipeline == response_msg->pipeline());
    sentMsg.erase(response_msg->id());

    cout<<"pipeline";
    for(size_t i = 0; i < pipeline.size(); i++)
        cout<<" "<<pipeline[i];
    cout<<"\nDataImport: #"<<(importBlocks - leftBlocks)<<" block of "<<importBlocks<<" blocks has been imported successfully\n\n";
    assert(sentMsg.empty());
    --leftBlocks;
    ns3::Simulator::ScheduleNow(&ImportRequest, leftBlocks);
}

void ImportResponse(ns3::Ptr<MsgInfo> request_msg)
{
    vector<HostName> pipeline = sentMsg.at(request_msg->id());
    reverse(pipeline.begin(), pipeline.end());
    MsgId id = getNetSim()->ImportResponse(request_msg->id(), pipeline, 1<<10, ns3::MakeCallback(&ImportFinish), NULL);
    assert(id != MsgId_invalid);
    sentMsg[id] = pipeline;
    sentMsg.erase(request_msg->id());
}

void ImportRequest(long blocks)
{
    if (blocks > 0) {
        vector<HostName> pipeline = findBlockDestination();
        pipeline.push_back("manager");
        reverse(pipeline.begin(), pipeline.end());
        assert(pipeline.size() == 4);
        MsgId id = getNetSim()->ImportRequest(pipeline, blockSize, ns3::MakeCallback(&ImportResponse), NULL);
        assert(id != MsgId_invalid);
        sentMsg[id] = pipeline;
    } else {
        // all the blocks of current file have been successfully imported, then schedule next data import
        switch (importType) {
            case RepeatedImport:
                blocks = importBlocks;
                break;
            case RandomImport:
                while((blocks = rand() % importBlocks) == 0);
                break;
            case IncrementalImport:
                importBlocks += blockStep;
                blocks = importBlocks;
                break;
            case DecrementalImport:
                blocks = (importBlocks - blockStep <= 0) ? importBlocks : (importBlocks - blockStep);
                importBlocks = blocks;
        }
        leftBlocks = blocks;
        blockPerTT += (long)ceil(blocks * 3.0 / getClusterSlaveNodes().size());
        ns3::Simulator::Schedule(ns3::MilliSeconds(importInterval), &ImportRequest, leftBlocks);
        //cout<<"Start a new DataImport\n";
    }
}

void enableDataImport(int importtype, long interval, long blocks, long size)
{
    assert(importType >= 0 && importType <= 3);
    assert(interval > 0 && blocks > 0 && (size == (1<<26)));

    importType = importtype;
    importInterval = interval;
    importBlocks = blocks;
    leftBlocks = blocks;
    blockSize = size;
    blockPerTT += (long)ceil(importBlocks * 3.0 / getClusterSlaveNodes().size());
    ns3::Simulator::Schedule(ns3::MilliSeconds(interval), &ImportRequest, leftBlocks);
}
