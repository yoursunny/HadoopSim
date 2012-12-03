/*
Lei Ye <leiy@cs.arizona.edu>
HadoopSim is a simulator for a Hadoop Runtime by replaying the collected traces.
*/
#ifndef SPLIT_H
#define SPLIT_H

#include <string>
#include <vector>

class Split {
public:
    Split(std::string splitId, std::vector<std::string> dataNodes);
    std::string getSplitId();
    std::vector<std::string> getDataNodes();
private:
    std::string splitId;        // splitId is the same as taskID
    std::vector<std::string> dataNodes;
};

#endif // SPLIT_H
