/*
Lei Ye <leiy@cs.arizona.edu>
HadoopSim is a simulator for a Hadoop Runtime by replaying the collected traces.
*/
#include <assert.h>
#include <stdlib.h>
#include <time.h>
#include <iostream>
#include "Cluster.h"
#include "HadoopSim.h"
#include "JobClient.h"
#include "JobTracker.h"
#include "Misc.h"
#include "NetMonitor.h"
#include "Status.h"
#include "TaskTracker.h"
#include "TraceAnalyzer.h"
#include "TraceReader.h"
using namespace std;

/* Sim Variables */
static int schedType = 0;
static int topoType = 0;
static string topologyFile;
static string traceFilePrefix;
static int numTraceFiles;
static bool needDebug = false;
static string debugDir;

void completeCluster(HadoopNetSim::NetSim *netsim)
{
    initJobTracker(getClusterMasterNodes().getHostName(), schedType);
    long firstJobSubmitTime = initTaskTrackers();
    initJobClient(Replay, firstJobSubmitTime, needDebug, debugDir);
}

void initSim()
{
    srand(time(NULL));
    initTraceReader(traceFilePrefix, numTraceFiles, needDebug, debugDir);
    setupCluster(topoType, topologyFile, needDebug, debugDir);
}

void runSim()
{
    cout<<"Hadoop Sim Running...\n";
    ns3::Simulator::Run();
    ns3::Simulator::Destroy();
}

void endSim()
{
    killJobClient();
    killTaskTrackers();
    killJobTracker();
}

void helper()
{
    cout<<"./HadoopSim schedType[0-default, 1-netOpt] topoType[0-star, 1-rackrow, 2-fattree] topologyFile traceFilePrefix numTraceFiles needDebug debugDir\n";
    cout<<"Each trace file represents a single job. For N trace files, the trace file name is represented "
        <<"as prefix0, prefix1, prefix2, ......\n";
}

int parseParameters(int argc, char *argv[])
{
    if (argc < 7) {
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
    if (needDebug)
        debugDir.assign(argv[7]);

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
        startAnalysis(false, debugDir);
        disableNetMonitor();
    }
    endSim();
    return 0;
}
