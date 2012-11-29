/*
Lei Ye <leiy@cs.arizona.edu>
HadoopSim is a simulator for a Hadoop Runtime by replaying the collected traces.
*/
#ifndef JOBTRACKER_H
#define JOBTRACKER_H

#include <map>
#include <vector>
#include <list>
#include "HScheduler.h"
#include "Job.h"
#include "HeartBeat.h"
#include "TaskTrackerStatus.h"
#include "JobTaskStory.h"
using namespace std;

class JobTracker {
public:
    JobTracker(HScheduler *sched);
    HScheduler* getScheduler();
    void updateBlockNodeMapping(string splitID, vector<string> dataNodes);
    void acceptNewJob(JobStory *jobStory, long now);
    void updateTaskTrackerStatus(HeartBeatReport report, long now);
    void updateTaskStatus(HeartBeatReport report, long now);
    HeartBeatResponse processHeartbeat(HeartBeatReport report, long now);
    map<string, Job> &getRunningJobs();
    map<string, Job> &getCompletedJobs();
    map<string, vector<string> > getNode2Block();
    map<string, vector<string> > getBlock2Node();

private:
    HScheduler *sched;
    list<TaskAction> taskActions;
    map<string, vector<string> > node2Block;    // trackerName <---> splitIDs on this tracker
    map<string, vector<string> > block2Node;    // splitID <---> trackerNames holding this split

    map<string, TaskTrackerStatus> allTaskTrackerStatus;    // trackerName <--> TaskTrackerStatus
    map<string, Job> runningJobs;
    map<string, Job> completedJobs;


//    map<string, Job> allJobs;   // jobID <--> Job
//    map<string, Task> allTasks; // taskID <--> Task
};

void initJobTracker(int schedType);
void killJobTracker();
JobTracker *getJobTracker();

#endif

