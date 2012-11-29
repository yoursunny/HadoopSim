/*
Lei Ye <leiy@cs.arizona.edu>
HadoopSim is a simulator for a Hadoop Runtime by replaying the collected traces.
*/
#ifndef JOBCLIENT_H
#define JOBCLIENT_H

#include "HEvent.h"
#include <vector>
using namespace std;

typedef enum JobSubmissionPolicy {
    Replay,
    Serial,
    Stress
}JobSubmissionPolicy;

class JobClient: public EventListener {
public:
    JobClient(JobSubmissionPolicy policy);
    void submitJob(long evtTime);
    void completeJob(long evtTime);
    void probeLoad(long evtTime);
    void handleNewEvent(long timestamp, EvtType type);
private:
    JobSubmissionPolicy policy;
    long lastSubmissionTime;
//    vector<Task> runningJobs;
//    vector<Task> completedJobs;
};

void initJobClient(JobSubmissionPolicy policy, long firstJobSubmitTime);
void killJobClient();
JobClient *getJobClient();

#endif

