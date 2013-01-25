#include <assert.h>
#include <algorithm>
#include <fstream>
#include "JobTracker.h"
#include "TraceAnalyzer.h"
#include "TraceReader.h"
using namespace std;

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
                       <<task.taskID<<","<<task.taskType<<","<<task.attempts[k].mapInputBytes<<endl;
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
//      assert(totalMapOutputBytes >= totalRedShuffleBytes);
    }
}

void logRawJobTaskDetails(deque<JobStory> &jobSet, string debugDir)
{
    ofstream csvFile;

    for(size_t i = 0; i < jobSet.size(); i++) {
        csvFile.open((debugDir + jobSet[i].jobID + "_RawDetails.csv").c_str());
        // log Job Detail
        // jobID, numMap, numReduce, submitTime, startTime, endTime
        csvFile<<"jobID,numMap,numReduce,submitTime,startTime,endTime"<<endl;
        csvFile<<jobSet[i].jobID<<","<<jobSet[i].totalMaps<<","<<jobSet[i].totalReduces<<","
               <<jobSet[i].submitTime<<","<<jobSet[i].launchTime<<","<<jobSet[i].finishTime<<endl<<endl;

        // log Task Detail
        // taskAttemptID, taskTracker, type, isRemote, dataSource, datasource1, datasource2, datasource3,
        // dataSize, startTime, finishTime, CPUTime, nettime+waitingtime, outputSize
        csvFile<<"taskD,taskTracker,type,isRemote,dataSource,datasource1,datasource2,datasource3,dataSize,startTime,finishTime,CPUTime,nettime+waitingtime,outputSize"<<endl;
        for(size_t j = 0; j < jobSet[i].mapTasks.size(); j++) {
            TaskStory taskStory = jobSet[i].mapTasks[j];
            size_t k;
            for(k = 0; k < taskStory.attempts.size(); k++) {
                if (taskStory.attempts[k].result.compare("SUCCESS") == 0)
                    break;
            }
            assert(k < taskStory.attempts.size());
            Attempt task = taskStory.attempts[k];

            vector<Location> nodes = taskStory.preferredLocations;
            string datasource1 = "null";
            string datasource2 = "null";
            string datasource3 = "null";
            if (nodes.size() >= 1) {
                datasource1 = nodes[0].hostName;
            }
            if (nodes.size() >= 2) {
                datasource2 = nodes[1].hostName;
            }
            if (nodes.size() >= 3) {
                datasource3 = nodes[2].hostName;
            }
            csvFile<<taskStory.taskID<<","<<task.location.hostName<<","
                   <<taskStory.taskType<<",na,"
                   <<"na,"<<datasource1<<","
                   <<datasource2<<","<<datasource3<<","<<task.mapInputBytes<<","
                   <<task.startTime<<","<<task.finishTime<<","
                   <<task.finishTime-task.startTime<<",0,"
                   <<task.mapOutputBytes<<endl;
        }
        csvFile<<endl;

        csvFile<<"taskD,taskTracker,type,isRemote,dataSource,datasource1,datasource2,datasource3,dataSize,startTime,finishTime,CPUTime,nettime+waitingtime,outputSize"<<endl;
        for(size_t j = 0; j < jobSet[i].reduceTasks.size(); j++) {
            TaskStory taskStory = jobSet[i].reduceTasks[j];
            size_t k;
            for(k = 0; k < taskStory.attempts.size(); k++) {
                if (taskStory.attempts[k].result.compare("SUCCESS") == 0)
                    break;
            }
            assert(k < taskStory.attempts.size());
            Attempt task = taskStory.attempts[k];
            csvFile<<taskStory.taskID<<","<<task.location.hostName<<","
                   <<taskStory.taskType<<",1,ALL,null,null,null,0,"
                   <<task.startTime<<","<<task.finishTime<<","
                   <<task.finishTime-task.shuffleFinished<<","<<(task.shuffleFinished - task.startTime)<<",0"<<endl;
        }
        csvFile.close();
    }
}

void logJobTaskDetails(const map<string, Job> &completedJobs, string debugDir)
{
    map<string, Job>::const_iterator jobIt;
    map<string, Task>::iterator taskIt;
    JobTracker *jobTracker = getJobTracker();
    map<string, vector<string>> block2Node = jobTracker->getBlock2Node();
    map<string, vector<string>>::iterator dataSourceIt;
    ofstream csvFile;

    for(jobIt = completedJobs.begin(); jobIt != completedJobs.end(); jobIt++) {
        Job job = jobIt->second;
        csvFile.open((debugDir + job.getJobID() + "_RuntimeDetails.csv").c_str());

        // log Job Detail
        // jobID, numMap, numReduce, submitTime, startTime, endTime
        csvFile<<"jobID,numMap,numReduce,submitTime,startTime,endTime"<<endl;
        csvFile<<job.getJobID()<<","<<job.getNumMap()<<","<<job.getNumReduce()<<","
               <<job.getSubmitTime()<<","<<job.getStarTime()<<","<<job.getEndTime()<<endl<<endl;

        // log Task Detail
        // taskAttemptID, taskTracker, type, isRemote, dataSource, datasource1, datasource2, datasource3,
        // dataSize, startTime, finishTime, CPUTime, nettime+waitingtime, outputSize
        csvFile<<"taskD,taskTracker,type,isRemote,dataSource,datasource1,datasource2,datasource3,dataSize,startTime,finishTime,CPUTime,nettime+waitingtime,outputSize"<<endl;
        map<string, Task> mapTasks = job.getCompletedMaps();
        for(taskIt = mapTasks.begin(); taskIt != mapTasks.end(); taskIt++) {
            TaskStatus taskStatus = taskIt->second.getTaskStatus();
            dataSourceIt = block2Node.find(taskStatus.taskAttemptID);
            vector<string> nodes = dataSourceIt->second;
            string datasource1 = "null";
            string datasource2 = "null";
            string datasource3 = "null";
            if (nodes.size() >= 1) {
                datasource1 = nodes[0];
            }
            if (nodes.size() >= 2) {
                datasource2 = nodes[1];
            }
            if (nodes.size() >= 3) {
                datasource3 = nodes[2];
            }
            csvFile<<taskStatus.taskAttemptID<<","<<taskStatus.taskTracker<<","
                   <<taskStatus.type<<","<<taskStatus.isRemote<<","
                   <<taskStatus.dataSource<<","<<datasource1<<","
                   <<datasource2<<","<<datasource3<<","<<taskStatus.dataSize<<","
                   <<taskStatus.startTime<<","<<taskStatus.finishTime<<","
                   <<taskStatus.duration<<","<<(taskStatus.finishTime - taskStatus.startTime - taskStatus.duration)<<","
                   <<taskStatus.outputSize<<endl;
        }
        csvFile<<endl;

        csvFile<<"taskD,taskTracker,type,isRemote,dataSource,datasource1,datasource2,datasource3,dataSize,startTime,finishTime,CPUTime,nettime+waitingtime,outputSize"<<endl;
        map<string, Task> reduceTasks = job.getCompletedReduces();
        for(taskIt = reduceTasks.begin(); taskIt != reduceTasks.end(); taskIt++) {
            TaskStatus taskStatus = taskIt->second.getTaskStatus();
            csvFile<<taskStatus.taskAttemptID<<","<<taskStatus.taskTracker<<","
                   <<taskStatus.type<<","<<taskStatus.isRemote<<","
                   <<taskStatus.dataSource<<","<<"null,null,null,"<<taskStatus.dataSize<<","
                   <<taskStatus.startTime<<","<<taskStatus.finishTime<<","
                   <<taskStatus.duration<<","<<(taskStatus.finishTime - taskStatus.startTime - taskStatus.duration)<<","
                   <<taskStatus.outputSize<<endl;
        }
        csvFile.close();
    }
}

void startAnalysis(bool isRawTrace, string debugDir)
{
    if (isRawTrace) {
        deque<JobStory> allJobs = getAllJobs();
        analyzeJobTaskExeTime(allJobs, debugDir);
        analyzeJobTaskTraffic(allJobs, debugDir);
        verifyShuffleData(allJobs, debugDir);
        logRawJobTaskDetails(allJobs, debugDir);
    } else {
        JobTracker *jobTracker = getJobTracker();
        if (!jobTracker->getCompletedJobs().empty()) {
            analyzeJobTaskExeTime(jobTracker->getCompletedJobs(), debugDir);
            logJobTaskDetails(jobTracker->getCompletedJobs(), debugDir);
        }
    }
}
