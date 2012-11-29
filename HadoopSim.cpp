/*
Lei Ye <leiy@cs.arizona.edu>
HadoopSim is a simulator for a Hadoop Runtime by replaying the collected traces.
*/
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <assert.h>
#include <time.h>
#include <iostream>
#include <climits>
#include <queue>
#include "HEvent.h"
#include "EventQueue.h"
#include "Status.h"
#include "JobTracker.h"
#include "TaskTracker.h"
#include "JobClient.h"
#include "TraceReader.h"
#include "TopologyReader.h"
#include "Cluster.h"
#include "Misc.h"
#include "TraceAnalyzer.h"
#include "ns3/Ns3.h"
using namespace std;

/* Sim Variables */
int schedType = 0;
int topoType = 0;
string topologyFile;
string traceFilePrefix;
int numTraceFiles;
bool needDebug = false;

void initSim()
{
    srand(time(NULL));
    initTraceReader(traceFilePrefix, numTraceFiles, needDebug);
    initTopologyReader(topologyFile, needDebug);
    initJobTracker(schedType);
    setupCluster(topoType);
    long firstJobSubmitTime = initTaskTrackers(0);
    initJobClient(Replay, firstJobSubmitTime);
}

void runSim()
{
    cout<<"Hadoop Sim Running...\n";
    Simulator::Run();
    Simulator::Destroy();
    cout<<"Hadoop Sim endTime = "<<Simulator::Now().GetMilliSeconds()<<endl;
}

void endSim()
{
    killJobClient();
    killTaskTrackers();
    killJobTracker();
}

void helper()
{
    cout<<"./HadoopSim schedType[0-default, 1-netOpt] topoType[0-star, 1-dual, 2-tree, 3-fattree] topologyFile traceFilePrefix numTraceFiles needDebug\n";
    cout<<"Each trace file represents a single job. For N trace files, the trace file name is represented "
        <<"as prefix0, prefix1, prefix2, ......\n";
}

int parseParameters(int argc, char *argv[])
{
    if (argc != 7) {
        helper();
        return Status::TooFewParameters;
    }

    schedType = atoi(argv[1]);
    if (schedType < 0 || schedType > 1) {
        helper();
        return Status::WrongParameters;
    }

    topoType = atoi(argv[2]);
    if (topoType < 0 || topoType > 3) {
        helper();
        return Status::WrongParameters;
    }

    // check whether files exist
    topologyFile.assign(argv[3]);
    if (access(topologyFile.c_str(), 0) < 0) {
        helper();
        return Status::NonExistentFile;
    }

    traceFilePrefix.assign(argv[4]);
    numTraceFiles = atoi(argv[5]);
    int nameLength = traceFilePrefix.size();
    if (numTraceFiles <= 0 || nameLength <= 0) {
        helper();
        return Status::WrongParameters;
    }

    needDebug = (atoi(argv[6]) == 0) ? false : true;

    for(int i = 0; i < numTraceFiles; i++) {
        traceFilePrefix.append(to_string(i));
        if (access(traceFilePrefix.c_str(), 0) < 0) {
            helper();
            return Status::NonExistentFile;
        }
        traceFilePrefix.resize(nameLength);
    }
    return 0;
}

int main(int argc, char *argv[])
{
    int ret;
    ret = parseParameters(argc, argv);
    if (ret < 0)
        return ret;

    initSim();
    runSim();
    getchar();

    // analyze simulation result
    if (needDebug) {
        startAnalysis(false);
    }

    endSim();
    return 0;
}
