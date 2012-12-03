/*
Lei Ye <leiy@cs.arizona.edu>
HadoopSim is a simulator for a Hadoop Runtime by replaying the collected traces.
*/
#ifndef MISC_H
#define MISC_H

#include <sstream>

inline std::string to_string(int n)
{
    std::stringstream ss;
    ss<<n;
    return ss.str();
}

#endif // MISC_H
