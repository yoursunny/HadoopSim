#ifndef JOBCLIENT_H
#define JOBCLIENT_H

#include <string>
#include "HEvent.h"

typedef enum JobSubmissionPolicy {
    Replay,     //replay the trace by following the job inter-arrival rate faithfully
    Serial,     //submitting jobs sequentially
    Stress      //ignore submission time, keep submitting jobs until the cluster is saturated.
}JobSubmissionPolicy;

class JobClient: public EventListener {
public:
    JobClient(JobSubmissionPolicy policy);
    void submitJob(long evtTime);
    void completeJob(long evtTime);
    void probeLoad(long evtTime);
    bool isSystemOverloaded(void);
    void handleNewEvent(EvtType type);
    long getProbingInterval(void);
private:
    JobSubmissionPolicy policy;
    long lastSubmissionTime;
    long loadProbingInterval;
};

void initJobClient(JobSubmissionPolicy policy, long firstJobSubmitTime, bool needDebug, std::string debugDir);
void killJobClient();
JobClient *getJobClient();

#endif // JOBCLIENT_H
