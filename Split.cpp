#include "Split.h"
using namespace std;

Split::Split(string splitId, vector<string> dataNodes)
{
    this->splitId = splitId;
    this->dataNodes = dataNodes;
}

string Split::getSplitId()
{
    return this->splitId;
}

vector<string> Split::getDataNodes()
{
    return this->dataNodes;
}
