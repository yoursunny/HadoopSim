#include <assert.h>
#include <algorithm>
#include <iostream>
#include "EventQueue.h"
#include "JobClient.h"
#include "JobTracker.h"
#include "TraceReader.h"
#include "netsim/netsim.h"
using namespace std;

/* JobClient Variables */
static JobClient *client;
static string debugDirectory;
static bool debugOption = false;
static double OVERLOAD_MAPTASK_MAPSLOT_RATIO = 2.0f;
static long LOAD_PROB_INTERVAL_START = 6000;    //ms
static long LOAD_PROB_INTERVAL_MAX = 120000;    //ms

JobClient::JobClient(JobSubmissionPolicy policy)
{
    this->policy = policy;
    this->lastSubmissionTime = 0;
    this->loadProbingInterval = LOAD_PROB_INTERVAL_START;
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
            //dumpJobStory(jobStory, debugDirectory);
        }

        // setup next event according to the specified policy
        if (isMoreJobs()) {
            if (this->policy == Replay) {
                long timeStamp = nextJobSubmitTime() - lastSubmissionTime;
                HEvent evt(this, EVT_JobSubmit);
                ns3::Simulator::Schedule(ns3::Seconds((double)timeStamp/1000.0), &hadoopEventCallback, evt);
            }
        }
    }
}

void JobClient::completeJob(long evtTime)
{
    if (isMoreJobs()) {
        if (this->policy == Serial) {
            JobStory jobStory = fetchNextJob();
            JobTracker *jobTracker = getJobTracker();
            jobTracker->acceptNewJob(&jobStory, evtTime);
        }
    }
}

void JobClient::probeLoad(long evtTime)
{
    if (isMoreJobs()) {
        if (isSystemOverloaded()) {
            loadProbingInterval = min(loadProbingInterval * 2, LOAD_PROB_INTERVAL_MAX);
            HEvent evt(client, EVT_LoadProbe);
            ns3::Simulator::Schedule(ns3::Seconds((double)(loadProbingInterval)/1000.0), &hadoopEventCallback, evt);
        } else {
            HEvent evt1(this, EVT_JobSubmit);
            ns3::Simulator::Schedule(ns3::Seconds(1.0), &hadoopEventCallback, evt1);

            loadProbingInterval = LOAD_PROB_INTERVAL_START;
            HEvent evt2(client, EVT_LoadProbe);
            ns3::Simulator::Schedule(ns3::Seconds((double)(loadProbingInterval)/1000.0), &hadoopEventCallback, evt2);
        }
    }
}

bool JobClient::isSystemOverloaded(void)
{
    JobTracker *jobTracker = getJobTracker();
    map<string, Job> &runningJobs = jobTracker->getRunningJobs();

    // If there are more jobs than number of task trackers, we assume the cluster is overloaded
    if (runningJobs.size() >= jobTracker->getTaskTrackerCount())
        return true;

    // check MAP tasks and MAP slots
    double incompleteMapTasks = 0.0f;
    map<string, Job>::iterator jobIt;
    for(jobIt = runningJobs.begin(); jobIt != runningJobs.end(); jobIt++) {
        Job job = jobIt->second;
        incompleteMapTasks += (1.0 - min((double)job.getCompletedMaps().size()/job.getNumMap(), 1.0)) * job.getNumMap();
    }

    cout<<"runningJobs = "<<runningJobs.size()<<", incompleteMapTasks = "<<incompleteMapTasks<<", value = "<<OVERLOAD_MAPTASK_MAPSLOT_RATIO * jobTracker->getMapSlotCapacity()<<endl;
    if (incompleteMapTasks > OVERLOAD_MAPTASK_MAPSLOT_RATIO * jobTracker->getMapSlotCapacity())
        return true;

    return false;
}

void JobClient::handleNewEvent(EvtType type)
{
    long timestamp = ns3::Simulator::Now().GetMilliSeconds();
    switch(type) {
        case EVT_JobSubmit:
            submitJob(timestamp);
            break;
        case EVT_JobDone:
            completeJob(timestamp);
            break;
        case EVT_LoadProbe:
            probeLoad(timestamp);
            break;
        default:
            cout<<"Unhandled Event for JobClient\n";
    }
}

long JobClient::getProbingInterval(void)
{
    return this->loadProbingInterval;
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
    HEvent evt(client, EVT_JobSubmit);
    ns3::Simulator::Schedule(ns3::Seconds((double)firstJobSubmitTime/1000.0), &hadoopEventCallback, evt);

    if (policy == Stress) {
        // add probeLoad Event
        HEvent evt(client, EVT_LoadProbe);
        ns3::Simulator::Schedule(ns3::Seconds((double)(firstJobSubmitTime+client->getProbingInterval())/1000.0), &hadoopEventCallback, evt);
    }
}

void killJobClient()
{
    delete client;
}

JobClient *getJobClient()
{
    return client;
}
