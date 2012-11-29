/*
Lei Ye <leiy@cs.arizona.edu>
HadoopSim is a simulator for a Hadoop Runtime by replaying the collected traces.
*/
#ifndef SPLIT_H
#define SPLIT_H

#include <vector>
#include <string>
using namespace std;

class Split {
public:
    Split(string splitId, vector<string> dataNodes);
    string getSplitId();
    vector<string> getDataNodes();
private:
    string splitId;        // splitId is the same as taskID
    vector<string> dataNodes;
};

#endif

