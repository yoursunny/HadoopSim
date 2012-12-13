/*
Lei Ye <leiy@cs.arizona.edu>
HadoopSim is a simulator for a Hadoop Runtime by replaying the collected traces.
*/
#ifndef JOBCLIENT_H
#define JOBCLIENT_H

#include <string>
#include "HEvent.h"

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
};

void initJobClient(JobSubmissionPolicy policy, long firstJobSubmitTime, bool needDebug, std::string debugDir);
void killJobClient();
JobClient *getJobClient();

#endif // JOBCLIENT_H
