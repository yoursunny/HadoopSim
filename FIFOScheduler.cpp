/*
Lei Ye <leiy@cs.arizona.edu>
HadoopSim is a simulator for a Hadoop Runtime by replaying the collected traces.
*/
#include <assert.h>
#include <iostream>
#include "HScheduler.h"
#include "JobTracker.h"
using namespace std;

list<TaskAction> FIFOScheduler::assignTasks(string trackerName, long numAvailMapSlots, long numAvailReduceSlots, long now)
{
    list<TaskAction> taskAction;
    TaskAction action;
    JobTracker *jobTracker = getJobTracker();

    map<string, vector<string> > block2Node = jobTracker->getBlock2Node();
    map<string, Task> waitingMaps;
    map<string, Task> waitingReduces;
    map<string, Job>::iterator jobIt;
    map<string, Task>::iterator taskIt;
    map<string, vector<string> >::iterator mapIt;

    // allocate map tasks first
    for(int i = 0; i < numAvailMapSlots; i++) {
        // if there is no map tasks left, leave silently
        map<string, Job> &runningJobs = jobTracker->getRunningJobs();
        if (runningJobs.size() == 0)
            break;
        for(jobIt = runningJobs.begin(); jobIt != runningJobs.end(); jobIt++) {
            if (jobIt->second.isWaitingMapLeft())
                break;
        }
        if (jobIt == runningJobs.end())
            break;

        // choose a Job
        Job job = jobIt->second;
        if (job.isFirstMap())
            job.setStartTime(now);

        // try to find a local task
        bool isTaskFound = false;
        waitingMaps = job.getWaitingMaps();
        for(taskIt = waitingMaps.begin(); taskIt != waitingMaps.end(); taskIt++) {
            mapIt = block2Node.find(taskIt->second.getTaskStatus().taskAttemptID);
            vector<string> nodes = mapIt->second;
            for(size_t j = 0; j < nodes.size(); j++) {
                if(nodes[j].compare(trackerName) == 0) {
                    isTaskFound = true;
                    break;
                }
            }
            if (isTaskFound)
                break;
        }
        if (isTaskFound) {  // we find a node-local map task
            action.type = LAUNCH_TASK;
            action.status = taskIt->second.getTaskStatus();
            action.status.taskTracker = trackerName;
            job.moveWaitingMapToRunning(false, trackerName, taskIt->first);
        } else {
            // we will assign a non-local map task to this task tracker
            taskIt = waitingMaps.begin();
            assert(taskIt != waitingMaps.end());
            action.type = LAUNCH_TASK;
            action.status = taskIt->second.getTaskStatus();
            action.status.taskTracker = trackerName;
            action.status.isRemote = true;
            job.moveWaitingMapToRunning(true, trackerName, taskIt->first);
        }
        jobIt->second = job;
        taskAction.push_back(action);
    }

    // for reduce tasks, However we _never_ assign more than 1 reduce task per heartbeat
    if (numAvailReduceSlots > 0) {
        map<string, Job> &runningJobs = jobTracker->getRunningJobs();
        if (runningJobs.size() == 0)
            goto done;
        for(jobIt = runningJobs.begin(); jobIt != runningJobs.end(); jobIt++) {
            if (jobIt->second.isWaitingReduceLeft())
                break;
        }
        if (jobIt == runningJobs.end())
            goto done;

        // choose a Job
        Job job = jobIt->second;

        // Ensure we have sufficient map outputs ready to shuffle before scheduling reduces
        if (!job.canScheduleReduce())
            goto done;

        // try to find a local task
        waitingReduces = job.getWaitingReduces();
        taskIt = waitingReduces.begin();
        assert(taskIt != waitingReduces.end());
        action.type = LAUNCH_TASK;
        action.status = taskIt->second.getTaskStatus();
        action.status.taskTracker = trackerName;
        job.moveWaitingReduceToRunning(trackerName, taskIt->first);
        jobIt->second = job;
        taskAction.push_back(action);
    }

done:
    return taskAction;
}
