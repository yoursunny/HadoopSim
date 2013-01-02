/*
Lei Ye <leiy@cs.arizona.edu>
HadoopSim is a simulator for a Hadoop Runtime by replaying the collected traces.
*/
#include <assert.h>
#include <math.h>
#include <iostream>
#include "EventQueue.h"
#include "JobClient.h"
#include "JobTracker.h"
#include "TraceReader.h"
#include "netsim/netsim.h"
using namespace std;

/* JobTracker Variables */
static JobTracker *jobTracker;
static FIFOScheduler fifoSched("FIFOScheduler");
static DataLocalityScheduler dataLocalitySched("DataLocalityScheduler");

JobTracker::JobTracker(string hostName, HScheduler *sched)
{
    this->hostName = hostName;
    this->sched = sched;
}

void JobTracker::updateBlockNodeMapping(string splitID, vector<string> dataNodes)
{
    map<string, vector<string>>::iterator it;
    assert(block2Node.find(splitID) == block2Node.end());
    block2Node.insert(pair<string, vector<string>>(splitID, dataNodes));

    for(size_t i = 0; i < dataNodes.size(); i++) {
        it = node2Block.find(dataNodes[i]);
        if (it != node2Block.end()) {
            it->second.push_back(splitID);
        } else {
            vector<string> blocks;
            blocks.push_back(splitID);
            node2Block.insert(pair<string, vector<string>>(dataNodes[i], blocks));
        }
    }
}

// accept the new job and create runtime Job data structure.
void JobTracker::acceptNewJob(JobStory *jobStory, long now)
{
    Job job(jobStory->jobID, jobStory->totalMaps, jobStory->totalReduces, now);
    job.initMapTasks(jobStory->mapTasks);
    job.initReduceTasks(jobStory->reduceTasks);
    runningJobs.insert(pair<string, Job>(jobStory->jobID, job));
}

void JobTracker::updateTaskTrackerStatus(HeartBeatReport report, long now)
{
    map<string, TaskTrackerStatus>::iterator it;
    it = allTaskTrackerStatus.find(report.hostName);
    if (it != allTaskTrackerStatus.end()) {
        TaskTrackerStatus trackerStatus = it->second;
        assert(trackerStatus.getLastReportTime() < now);
        trackerStatus.setLastReportTime(now);
        trackerStatus.setAvailMapSlots(report.numAvailMapSlots);
        trackerStatus.setAvailReduceSlots(report.numAvailReduceSlots);
        it->second = trackerStatus;
    } else {
        // initial HeartBeat report from the TaskTracker
        TaskTrackerStatus trackerStatus(report.hostName);
        trackerStatus.setLastReportTime(now);
        trackerStatus.setAvailMapSlots(report.numAvailMapSlots);
        trackerStatus.setAvailReduceSlots(report.numAvailReduceSlots);
        allTaskTrackerStatus.insert(pair<string, TaskTrackerStatus>(report.hostName, trackerStatus));
    }
}

void JobTracker::updateTaskStatus(HeartBeatReport report, long now)
{
    list<TaskStatus> taskStatus = report.taskStatus;
    for(list<TaskStatus>::iterator taskStatusIt = taskStatus.begin(); taskStatusIt != taskStatus.end(); taskStatusIt++) {

        map<string, Job>::iterator jobIt = runningJobs.find(taskStatusIt->jobID);
        assert(jobIt != runningJobs.end() && jobIt->second.getState() == JOBRUNNING);

        TaskStatus status = *taskStatusIt;
        Job job = jobIt->second;
        ActionType type = job.updateTaskStatus(status);
        jobIt->second = job;
        if (type != NO_ACTION && type != FETCH_MAPDATA) {
            TaskAction action;
            action.type = type;
            action.status = status;
            taskActions.push_back(action);       //add KILL_TASK & START_REDUCEPHASE actions
        }
        if (type == FETCH_MAPDATA) {
            // this Map is done, try to tell all running Reduce tasks to fetch its data
            assert(status.type == MAPTASK);
            assert(status.outputSize > 0);

            // data information
            MapDataAction dataAction;
            dataAction.dataSource = report.hostName;
            dataAction.dataSize = ceil(status.outputSize * 1.0 / job.getNumReduce());

            // try to set DataAction for running Reduce tasks
            map<string, Task> runningReduces = job.getRunningReduces();
            map<string, Task>::iterator taskIt = runningReduces.begin();
            while(taskIt != runningReduces.end()) {
                string nodeName = taskIt->second.getTaskStatus().taskTracker;
                map<string, vector<MapDataAction>>::iterator dataActionIt;
                dataActionIt = allMapDataActions.find(nodeName);
                if (dataActionIt != allMapDataActions.end()) {
                    vector<MapDataAction> mapDataActions = dataActionIt->second;
                    dataAction.reduceTaskID = taskIt->first;
                    mapDataActions.push_back(dataAction);
                    dataActionIt->second = mapDataActions;
                } else {
                    vector<MapDataAction> mapDataActions;
                    dataAction.reduceTaskID = taskIt->first;
                    mapDataActions.push_back(dataAction);
                    allMapDataActions.insert(pair<string, vector<MapDataAction>>(nodeName, mapDataActions));
                }
                taskIt++;
            }
        }

        // if job is done, move it to completedJobs
        if(jobIt->second.isSucceeded()) {
            job = jobIt->second;
            job.setEndTime(now);    // set finish time
            jobIt->second = job;

            completedJobs.insert(pair<string, Job>(jobIt->first, jobIt->second));
            runningJobs.erase(jobIt);

            cout<<"xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx\n";
            // also update TraceReader completedJobSet
            completeJob(jobIt->first);

            // ask for a new job
            JobClient *client = getJobClient();
            HEvent evt(client, EVT_JobDone);
            ns3::Simulator::Schedule(ns3::Seconds((double)1/1000.0), &hadoopEventCallback, evt);
        }
    }
}

HeartBeatResponse JobTracker::processHeartbeat(HeartBeatReport report, long now)
{
    assert(report.type == HBReport);
    taskActions.clear();

    updateTaskTrackerStatus(report, now);
    updateTaskStatus(report, now);

    vector<MapDataAction> mapDataActions;
    if (report.numAvailMapSlots > 0 || report.numAvailReduceSlots > 0) {
        // add LAUNCH_TASK actions, scheduling a task
        list<TaskAction> actions = sched->assignTasks(report.hostName, report.numAvailMapSlots, report.numAvailReduceSlots, now);
        while (!actions.empty()) {
            // add to TaskAction which will be sent to the TaskTracker who send this HeartBeatReport
            TaskStatus status = actions.front().status;
            taskActions.push_back(actions.front());
            actions.pop_front();

            if (status.type == REDUCETASK) { // a new launched Reduce Task, find available Map data for it
                map<string, Job>::iterator jobIt = runningJobs.find(status.jobID);
                assert(jobIt != runningJobs.end() && jobIt->second.getState() == JOBRUNNING);

                // find all completed Map Tasks since they have all data ready to use
                Job job = jobIt->second;
                map<string, Task> completedMaps = job.getCompletedMaps();
                map<string, Task>::iterator taskIt = completedMaps.begin();
                while(taskIt != completedMaps.end()) {
		            if (taskIt->second.getTaskStatus().outputSize > 0) {
                        MapDataAction dataAction;
                        dataAction.reduceTaskID = status.taskAttemptID;
                        dataAction.dataSource = taskIt->second.getTaskStatus().taskTracker;
                        dataAction.dataSize = ceil(taskIt->second.getTaskStatus().outputSize * 1.0 / job.getNumReduce());
                        mapDataActions.push_back(dataAction);
		            } else {
			            job.removeMapDataSource();
		            }
                    taskIt++;
                }
		        runningJobs[jobIt->first] = job;
            }
        }
    }

    HeartBeatResponse response;
    response.type = HBResponse;
    response.taskActions = taskActions;

    // combine MapDataAction from two different cases
    map<string, vector<MapDataAction>>::iterator dataActionIt = allMapDataActions.find(report.hostName);
    if (dataActionIt != allMapDataActions.end()) {
        vector<MapDataAction> actions = dataActionIt->second;
        response.mapDataActions = actions;
        actions.clear();
        dataActionIt->second = actions;

        while(!mapDataActions.empty()) {
            response.mapDataActions.push_back(mapDataActions.back());
            mapDataActions.pop_back();
        }
    } else {
        response.mapDataActions = mapDataActions;
    }
    return response;
}

map<string, Job> &JobTracker::getRunningJobs()
{
    return this->runningJobs;
}

const map<string, Job> &JobTracker::getCompletedJobs() const
{
    return this->completedJobs;
}

const map<std::string, vector<string>> &JobTracker::getNode2Block() const
{
    return this->node2Block;
}

const map<string, vector<string>> &JobTracker::getBlock2Node() const
{
    return this->block2Node;
}

const std::string JobTracker::getHostName(void) const
{
    return this->hostName;
}

void initJobTracker(string hostName, int schedType)
{
    assert(schedType == 0 || schedType == 1);
    switch(schedType) {
        case 0:
            jobTracker = new JobTracker(hostName, &fifoSched);
            break;
        case 1:
            jobTracker = new JobTracker(hostName, &dataLocalitySched);
            break;
        default:
            jobTracker = new JobTracker(hostName, &fifoSched);
            break;
    }
}

void killJobTracker(void)
{
    delete jobTracker;
}

JobTracker *getJobTracker(void)
{
    return jobTracker;
}

const std::string getJobTrackerName(void)
{
    return getJobTracker()->getHostName();
}
