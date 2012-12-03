/*
Lei Ye <leiy@cs.arizona.edu>
HadoopSim is a simulator for a Hadoop Runtime by replaying the collected traces.
*/
#ifndef TRACEREADER_H
#define TRACEREADER_H

#include <deque>
#include <string>
#include "JobTaskStory.h"

void dumpJobStory(JobStory *job, int ident = 0);
void initTraceReader(std::string traceFilePrefix, int numTraceFiles, bool debug);
bool isAllJobsDone();
bool isMoreJobs();
long nextJobSubmitTime();
JobStory fetchNextJob();
void completeJob(std::string jobID);
std::deque<JobStory> getAllJobs();

#endif // TRACEREADER_H
