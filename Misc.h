/*
Lei Ye <leiy@cs.arizona.edu>
HadoopSim is a simulator for a Hadoop Runtime by replaying the collected traces.
*/
#ifndef MISC_H
#define MISC_H

#include <sstream>
using namespace std;

inline string to_string(int n)
{
    stringstream ss;
    ss<<n;
    return ss.str();
}

#endif

