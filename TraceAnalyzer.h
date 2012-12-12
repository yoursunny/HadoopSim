/*
Lei Ye <leiy@cs.arizona.edu>
HadoopSim is a simulator for a Hadoop Runtime by replaying the collected traces.
*/
#ifndef TRACEANALYZER_H
#define TRACEANALYZER_H

#include <string>

void startAnalysis(bool isRawTrace, std::string debugDir);

#endif // TRACEANALYZER_H
