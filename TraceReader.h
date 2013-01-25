#ifndef TRACEREADER_H
#define TRACEREADER_H

#include <deque>
#include <string>
#include "JobTaskStory.h"

void dumpJobStory(JobStory &job, std::string debugDir);
void initTraceReader(std::string traceFilePrefix, int numTraceFiles, bool debug, std::string debugDir);
bool isAllJobsDone();
bool isMoreJobs();
long nextJobSubmitTime();
JobStory fetchNextJob();
void completeJob(std::string jobID);
std::deque<JobStory> getAllJobs();

#endif // TRACEREADER_H
