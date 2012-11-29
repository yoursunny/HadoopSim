/*
Lei Ye <leiy@cs.arizona.edu>
HadoopSim is a simulator for a Hadoop Runtime by replaying the collected traces.
*/
#include <assert.h>
#include <iostream>
#include "JobTracker.h"
#include "JobClient.h"
#include "EventQueue.h"
#include "TraceReader.h"
#include "ns3/Ns3.h"
using namespace std;

/* JobTracker Variables */
static JobTracker *jobTracker;

JobTracker::JobTracker(HScheduler *sched)
{
    this->sched = sched;
}

HScheduler* JobTracker::getScheduler()
{
    return this->sched;
}

void JobTracker::updateBlockNodeMapping(string splitID, vector<string> dataNodes)
{
    map<string, vector<string> >::iterator it;
    assert(block2Node.find(splitID) == block2Node.end());
    block2Node.insert(pair<string, vector<string> >(splitID, dataNodes));

    for(size_t i = 0; i < dataNodes.size(); i++) {
        it = node2Block.find(dataNodes[i]);
        if (it != node2Block.end()) {
            it->second.push_back(splitID);
        } else {
            vector<string> blocks;
            blocks.push_back(splitID);
            node2Block.insert(pair<string, vector<string> >(dataNodes[i], blocks));
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
        if (type != NO_ACTION) {
            TaskAction action;
            action.type = type;
            action.status = status;
            taskActions.push_back(action);       //add KILL_TASK & START_REDUCEPHASE actions
        }
        jobIt->second = job;

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
            HEvent evt(client, EVT_JobDone, now);
            Simulator::Schedule(Seconds((double)now/1000.0), &hadoopEventCallback, evt);
//            addEventToEvtQueue(evt);
        }
    }
}

/*
void createTaskEntry(TaskAttemptID taskid, String taskTracker, TaskInProgress tip);
void removeTaskEntry(TaskAttemptID taskid);
void markCompletedTaskAttempt(String taskTracker, TaskAttemptID taskid);
void markCompletedJob(JobInProgress job);
void removeMarkedTasks(String taskTracker);
void removeJobTasks(JobInProgress job);
void finalizeJob(JobInProgress job);

void addNewTracker(TaskTrackerStatus status);


JobStatus submitJob(JobID jobId);
void initJob(JobInProgress job);
JobStatus getJobStatus(JobID jobid);
TaskReport[] getMapTaskReports(JobID jobid);
TaskReport[] getReduceTaskReports(JobID jobid);
TaskReport[] getCleanupTaskReports(JobID jobid);
TaskReport[] getSetupTaskReports(JobID jobid);
TaskCompletionEvent[] getTaskCompletionEvents(JobID jobid, int fromEventId, int maxEvents);

void updateTaskStatuses(TaskTrackerStatus status);
*/

HeartBeatResponse JobTracker::processHeartbeat(HeartBeatReport report, long now)
{
    assert(report.type == HBReport);
    taskActions.clear();

    updateTaskTrackerStatus(report, now);
    updateTaskStatus(report, now);

    if (report.numAvailMapSlots > 0 || report.numAvailReduceSlots > 0) {
        // add LAUNCH_TASK actions, scheduling a task
        list<TaskAction> actions = sched->assignTasks(report.hostName, report.numAvailMapSlots, report.numAvailReduceSlots, now);
        while (!actions.empty()) {
            taskActions.push_back(actions.front());
            actions.pop_front();
        }
    }

    HeartBeatResponse response;
    response.type = HBResponse;
    response.taskActions = taskActions;
    return response;
}

map<string, Job> &JobTracker::getRunningJobs()
{
    return this->runningJobs;
}

map<string, Job> &JobTracker::getCompletedJobs()
{
    return this->completedJobs;
}

map<string, vector<string> > JobTracker::getNode2Block()
{
    return this->node2Block;
}

map<string, vector<string> > JobTracker::getBlock2Node()
{
    return this->block2Node;
}

FIFOScheduler fifoSched("FIFOScheduler");
DataLocalityScheduler dataLocalitySched("DataLocalityScheduler");
void initJobTracker(int schedType)
{
    assert(schedType == 0 || schedType == 1);
    if (schedType == 0) {
        jobTracker = new JobTracker(&fifoSched);
    } else {
        jobTracker = new JobTracker(&dataLocalitySched);
    }
}

void killJobTracker()
{
    delete jobTracker;
}

JobTracker *getJobTracker()
{
    return jobTracker;
}

