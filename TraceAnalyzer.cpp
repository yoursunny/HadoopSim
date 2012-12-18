/*
Lei Ye <leiy@cs.arizona.edu>
HadoopSim is a simulator for a Hadoop Runtime by replaying the collected traces.
*/
#include <assert.h>
#include <algorithm>
#include <fstream>
#include "JobTracker.h"
#include "TraceAnalyzer.h"
#include "TraceReader.h"
using namespace std;

static long HDFSBlockSize = (1<<26);

void getTimeStatistics(map<string, long> &timeSet, long &minTime, long &maxTime, long &medianTime)
{
    vector<long> exeTime;
    map<string, long>::iterator timeIt;

    for(timeIt = timeSet.begin(); timeIt != timeSet.end(); timeIt++) {
        exeTime.push_back(timeIt->second);
    }
    sort(exeTime.begin(), exeTime.end());

    if (exeTime.empty()) {
        minTime = maxTime = medianTime = 0;
    } else {
        size_t size = exeTime.size();
        minTime = exeTime[0];
        maxTime = exeTime[size - 1];
        medianTime = (size % 2) ? exeTime[size>>1] : (exeTime[(size>>1) - 1] + exeTime[size>>1])/2 ;
    }
}

void analyzeJobTaskExeTime(const map<string, Job> &completedJobs, string debugDir)
{
    map<string, long> jobTime;
    map<string, long> mapTaskTime;
    map<string, long> reduceTaskTime;
    map<string, long>::iterator timeIt;
    map<string, Job>::const_iterator jobIt;
    map<string, Task>::iterator taskIt;
    long minTime, maxTime, medianTime;
    ofstream csvFile;

    for(jobIt = completedJobs.begin(); jobIt != completedJobs.end(); jobIt++) {
        Job job = jobIt->second;
        jobTime.insert(pair<string, long>(job.getJobID(), job.getEndTime() - job.getStarTime()));

        csvFile.open((debugDir + job.getJobID() + "_RealExeTime.csv").c_str());
        mapTaskTime.clear();
        map<string, Task> mapTasks = job.getCompletedMaps();
        for(taskIt = mapTasks.begin(); taskIt != mapTasks.end(); taskIt++) {
            TaskStatus taskStatus = taskIt->second.getTaskStatus();
            mapTaskTime.insert(pair<string, long>(taskStatus.taskAttemptID, taskStatus.finishTime - taskStatus.startTime));
        }
        getTimeStatistics(mapTaskTime, minTime, maxTime, medianTime);
        csvFile<<"MapTaskTime,,minTime,"<<minTime<<",,maxTime,"<<maxTime<<",,medianTime,"<<medianTime<<endl;
        for(timeIt = mapTaskTime.begin(); timeIt != mapTaskTime.end(); timeIt++) {
            csvFile<<timeIt->first<<","<<timeIt->second<<endl;
        }
        csvFile<<endl;

        reduceTaskTime.clear();
        map<string, Task> reduceTasks = job.getCompletedReduces();
        for(taskIt = reduceTasks.begin(); taskIt != reduceTasks.end(); taskIt++) {
            TaskStatus taskStatus = taskIt->second.getTaskStatus();
            reduceTaskTime.insert(pair<string, long>(taskStatus.taskAttemptID, taskStatus.finishTime - taskStatus.startTime));
        }
        getTimeStatistics(reduceTaskTime, minTime, maxTime, medianTime);
        csvFile<<"ReduceTaskTime,,minTime,"<<minTime<<",,maxTime,"<<maxTime<<",,medianTime,"<<medianTime<<endl;
        for(timeIt = reduceTaskTime.begin(); timeIt != reduceTaskTime.end(); timeIt++) {
            csvFile<<timeIt->first<<","<<timeIt->second<<endl;
        }
        csvFile.close();
    }

    csvFile.open((debugDir + "AllJobs_RealExeTime.csv").c_str());
    getTimeStatistics(jobTime, minTime, maxTime, medianTime);
    csvFile<<"JobTime,,minTime,"<<minTime<<",,maxTime,"<<maxTime<<",,medianTime,"<<medianTime<<endl;
    for(timeIt = jobTime.begin(); timeIt != jobTime.end(); timeIt++) {
        csvFile<<timeIt->first<<","<<timeIt->second<<endl;
    }
    csvFile.close();
}

void analyzeJobTaskExeTime(deque<JobStory> &jobSet, string debugDir)
{
    map<string, long> jobTime;
    map<string, long> mapTaskTime;
    map<string, long> reduceTaskTime;
    map<string, long>::iterator timeIt;
    long minTime, maxTime, medianTime;
    ofstream csvFile;

    for(size_t i = 0; i < jobSet.size(); i++) {
        jobTime.insert(pair<string, long>(jobSet[i].jobID, jobSet[i].finishTime - jobSet[i].launchTime));

        csvFile.open((debugDir + jobSet[i].jobID + "_RawExeTime.csv").c_str());
        mapTaskTime.clear();
        for(size_t j = 0; j < jobSet[i].mapTasks.size(); j++) {
            TaskStory task = jobSet[i].mapTasks[j];
            mapTaskTime.insert(pair<string, long>(task.taskID, task.finishTime - task.startTime));
        }
        getTimeStatistics(mapTaskTime, minTime, maxTime, medianTime);
        csvFile<<"MapTaskTime,,minTime,"<<minTime<<",,maxTime,"<<maxTime<<",,medianTime,"<<medianTime<<endl;
        for(timeIt = mapTaskTime.begin(); timeIt != mapTaskTime.end(); timeIt++) {
            csvFile<<timeIt->first<<","<<timeIt->second<<endl;
        }
        csvFile<<endl;

        reduceTaskTime.clear();
        for(size_t j = 0; j < jobSet[i].reduceTasks.size(); j++) {
            TaskStory task = jobSet[i].reduceTasks[j];
            reduceTaskTime.insert(pair<string, long>(task.taskID, task.finishTime - task.startTime));
        }
        getTimeStatistics(reduceTaskTime, minTime, maxTime, medianTime);
        csvFile<<"ReduceTaskTime,,minTime,"<<minTime<<",,maxTime,"<<maxTime<<",,medianTime,"<<medianTime<<endl;
        for(timeIt = reduceTaskTime.begin(); timeIt != reduceTaskTime.end(); timeIt++) {
            csvFile<<timeIt->first<<","<<timeIt->second<<endl;
        }
        csvFile.close();
    }

    csvFile.open((debugDir + "AllJobs_RawExeTime.csv").c_str());
    getTimeStatistics(jobTime, minTime, maxTime, medianTime);
    csvFile<<"JobTime,,minTime,"<<minTime<<",,maxTime,"<<maxTime<<",,medianTime,"<<medianTime<<endl;
    for(timeIt = jobTime.begin(); timeIt != jobTime.end(); timeIt++) {
        csvFile<<timeIt->first<<","<<timeIt->second<<endl;
    }
    csvFile.close();
}

void analyzeJobTaskTraffic(deque<JobStory> &jobSet, string debugDir)
{
    ofstream csvFile;
    size_t k, m;

    for(size_t i = 0; i < jobSet.size(); i++) {
        csvFile.open((debugDir + jobSet[i].jobID + "_RawTraffic.csv").c_str());

        // map tasks
        for(size_t j = 0; j < jobSet[i].mapTasks.size(); j++) {
            TaskStory task = jobSet[i].mapTasks[j];
            if (task.preferredLocations.empty())    // map tasks for generating data, so pass
                continue;

            for(k = 0; k < task.attempts.size(); k++) {
                if (task.attempts[k].result.compare("SUCCESS") == 0)
                    break;
            }
            assert(k < task.attempts.size());
            for(m = 0; m < task.preferredLocations.size(); m++) {
                if (task.preferredLocations[m].rack.compare(task.attempts[k].location.rack) == 0
                    && task.preferredLocations[m].hostName.compare(task.attempts[k].location.hostName) == 0)
                    break;
            }
            if (m >= task.preferredLocations.size()) {
                // remotely run
                csvFile<<task.attempts[k].startTime<<","<<task.attempts[k].finishTime<<"," \
                       <<task.taskID<<","<<task.taskType<<","<<HDFSBlockSize<<endl;
            }
        }
        // reduce tasks
        for(size_t j = 0; j < jobSet[i].reduceTasks.size(); j++) {
            TaskStory task = jobSet[i].reduceTasks[j];
            for(k = 0; k < task.attempts.size(); k++) {
                if (task.attempts[k].result.compare("SUCCESS") == 0)
                    break;
            }
            assert(k < task.attempts.size());
            if (task.attempts[k].reduceShuffleBytes > 0)
                csvFile<<task.attempts[k].startTime<<","<<task.attempts[k].finishTime<<"," \
                       <<task.taskID<<","<<task.taskType<<","<<task.attempts[k].reduceShuffleBytes<<endl;
        }
        csvFile.close();
    }
}

void verifyShuffleData(deque<JobStory> &jobSet, string debugDir)
{
    ofstream txtFile;
    size_t k;
    long totalMapOutputBytes;
    long totalRedShuffleBytes;

    for(size_t i = 0; i < jobSet.size(); i++) {
        txtFile.open((debugDir + jobSet[i].jobID + "_ShuffleData.txt").c_str());
        if (jobSet[i].reduceTasks.empty()) {
            txtFile.close();
            continue;
        }

        totalMapOutputBytes = totalRedShuffleBytes = 0;

        // compute total map tasks output data size
        for(size_t j = 0; j < jobSet[i].mapTasks.size(); j++) {
            TaskStory task = jobSet[i].mapTasks[j];
            for(k = 0; k < task.attempts.size(); k++) {
                if (task.attempts[k].result.compare("SUCCESS") == 0)
                    break;
            }
            assert(k < task.attempts.size());
            if (task.attempts[k].mapOutputBytes > 0)
                totalMapOutputBytes += task.attempts[k].mapOutputBytes;
        }

        // compute total reduce tasks shuffle data size
        for(size_t j = 0; j < jobSet[i].reduceTasks.size(); j++) {
            TaskStory task = jobSet[i].reduceTasks[j];
            for(k = 0; k < task.attempts.size(); k++) {
                if (task.attempts[k].result.compare("SUCCESS") == 0)
                    break;
            }
            assert(k < task.attempts.size());
            if (task.attempts[k].reduceShuffleBytes > 0)
                totalRedShuffleBytes += task.attempts[k].reduceShuffleBytes;
        }

        txtFile<<"totalMapOutputBytes="<<totalMapOutputBytes<<", totalRedShuffleBytes="<<totalRedShuffleBytes<<endl;
        txtFile.close();
        assert(totalMapOutputBytes >= totalRedShuffleBytes);
    }
}

void startAnalysis(bool isRawTrace, string debugDir)
{
    if (isRawTrace) {
        deque<JobStory> allJobs = getAllJobs();
        analyzeJobTaskExeTime(allJobs, debugDir);
        analyzeJobTaskTraffic(allJobs, debugDir);
        verifyShuffleData(allJobs, debugDir);
    } else {
        JobTracker *jobTracker = getJobTracker();
        if (!jobTracker->getCompletedJobs().empty())
            analyzeJobTaskExeTime(jobTracker->getCompletedJobs(), debugDir);
    }
}
