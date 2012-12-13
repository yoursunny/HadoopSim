/*
Lei Ye <leiy@cs.arizona.edu>
HadoopSim is a simulator for a Hadoop Runtime by replaying the collected traces.
*/
#include <assert.h>
#include <iostream>
#include "EventQueue.h"
#include "JobClient.h"
#include "JobTracker.h"
#include "TraceReader.h"
#include "ns3/Ns3.h"
using namespace ns3;
using namespace std;

/* JobTracker Variables */
static JobClient *client;
static string debugDirectory;
static bool debugOption = false;

JobClient::JobClient(JobSubmissionPolicy policy)
{
    this->policy = policy;
    this->lastSubmissionTime = 0;
}

void JobClient::submitJob(long evtTime)
{
    if (isMoreJobs()) {
        JobStory jobStory = fetchNextJob();

        lastSubmissionTime = jobStory.submitTime;
        cout<<"New JOB ID = "<<jobStory.jobID<<endl;

        JobTracker *jobTracker = getJobTracker();
        jobTracker->acceptNewJob(&jobStory, evtTime);

        if (debugOption) {
            dumpJobStory(jobStory, debugDirectory);
        }

        // setup next event according to the specified policy
        if (this->policy == Replay) {
            if (isMoreJobs()) {
                long timeStamp = nextJobSubmitTime() - lastSubmissionTime + evtTime;
                HEvent evt(this, EVT_JobSubmit, timeStamp);
                Simulator::Schedule(Seconds((double)timeStamp/1000.0), &hadoopEventCallback, evt);
            }
        }
        else if (this->policy == Stress) {
            // todo
        }
    }
}

void JobClient::completeJob(long evtTime)
{
    if (isMoreJobs()) {
        if (this->policy == Serial) {
            JobStory jobStory = fetchNextJob();
            JobTracker *jobTracker = getJobTracker();
            jobTracker->acceptNewJob(&jobStory, evtTime + 1);
        }
        else if (this->policy == Stress) {
            // todo
        }
    }
}

void JobClient::probeLoad(long evtTime)
{

}

void JobClient::handleNewEvent(long timestamp, EvtType type)
{
    switch(type) {
        case EVT_JobSubmit:
            submitJob(timestamp);
            break;
        case EVT_JobDone:
            completeJob(timestamp);
            break;
        default:
            cout<<"Unhandled Event for JobClient\n";
    }
}

void initJobClient(JobSubmissionPolicy policy, long firstJobSubmitTime, bool needDebug, string debugDir)
{
    assert(Replay <= policy && policy <= Stress);
    client = new JobClient(policy);

    if (needDebug) {
        debugOption = needDebug;
        debugDirectory = debugDir;
    }

    // add first Job Submission event to the EventQueue
    HEvent evt(client, EVT_JobSubmit, firstJobSubmitTime);
    Simulator::Schedule(Seconds((double)firstJobSubmitTime/1000.0), &hadoopEventCallback, evt);
}

void killJobClient()
{
    delete client;
}

JobClient *getJobClient()
{
    return client;
}
