/*
Lei Ye <leiy@cs.arizona.edu>
HadoopSim is a simulator for a Hadoop Runtime by replaying the collected traces.
*/
#ifndef JOBTRACKER_H
#define JOBTRACKER_H

#include <list>
#include <map>
#include <vector>
#include "HeartBeat.h"
#include "HScheduler.h"
#include "Job.h"
#include "JobTaskStory.h"
#include "TaskTrackerStatus.h"

class JobTracker {
public:
    JobTracker(HScheduler *sched);
    HScheduler* getScheduler();
    void updateBlockNodeMapping(std::string splitID, std::vector<std::string> dataNodes);
    void acceptNewJob(JobStory *jobStory, long now);
    void updateTaskTrackerStatus(HeartBeatReport report, long now);
    void updateTaskStatus(HeartBeatReport report, long now);
    HeartBeatResponse processHeartbeat(HeartBeatReport report, long now);
    std::map<std::string, Job> &getRunningJobs();
    std::map<std::string, Job> &getCompletedJobs();
    std::map<std::string, std::vector<std::string> > getNode2Block();
    std::map<std::string, std::vector<std::string> > getBlock2Node();
private:
    HScheduler *sched;
    std::list<TaskAction> taskActions;
    std::map<std::string, std::vector<std::string> > node2Block;    // trackerName <---> splitIDs on this tracker
    std::map<std::string, std::vector<std::string> > block2Node;    // splitID <---> trackerNames holding this split
    std::map<std::string, TaskTrackerStatus> allTaskTrackerStatus;  // trackerName <--> TaskTrackerStatus
    std::map<std::string, std::vector<MapDataAction> > allMapDataActions;  // trackerName <--> vector<MapDataAction> on this tracker
    std::map<std::string, Job> runningJobs;
    std::map<std::string, Job> completedJobs;
};

void initJobTracker(int schedType);
void killJobTracker();
JobTracker *getJobTracker();

#endif // JOBTRACKER_H
