/*
Lei Ye <leiy@cs.arizona.edu>
HadoopSim is a simulator for a Hadoop Runtime by replaying the collected traces.
*/
#ifndef TOPOLOGYREADER_H
#define TOPOLOGYREADER_H

#include <vector>
using namespace std;

typedef struct HadoopHost {
    string hostName;
    string rackName;
    string ipAddr;
}HadoopHost;

typedef struct HadoopRack {
    string rackName;
    vector<HadoopHost> hostSet;
}HadoopRack;

typedef struct ClusterTopo {
    string clusterName;
    vector<HadoopRack> rackSet;
}ClusterTopo;

void initTopologyReader(string topologyFile, bool debug);
vector<HadoopHost> getHostTopology();

#endif
