/*
Lei Ye <leiy@cs.arizona.edu>
HadoopSim is a simulator for a Hadoop Runtime by replaying the collected traces.
*/
#include <assert.h>
#include <stdio.h>
#include <iostream>
#include "Misc.h"
#include "TraceAnalyzer.h"
#include "TraceReader.h"
#include "json/json.h"
using namespace std;

/* TraceReader Variables */
static deque<JobStory> jobSet;
static deque<JobStory> runningJobSet;
static deque<JobStory> completedJobSet;

#define IDENT(n) for (int i = 0; i < n; ++i) printf("    ")
#define Next(n) ((n) = ((n)->next_sibling))

Location parseSingleLocation(json_value *value)
{
    assert(value);
    Location location;
    json_value *it = value->first_child->first_child;
    location.rack.assign(it->string_value); Next(it);
    location.hostName.assign(it->string_value);
    return location;
}

vector<Attempt> parseAttempts(json_value *value)
{
    assert(value);
    vector<Attempt> attemptSet;
    for(json_value *it = value->first_child; it; Next(it)) {
        Attempt attempt;
        json_value *child = it->first_child;
        attempt.location = parseSingleLocation(child); Next(child);
        attempt.hostName.assign(child->string_value); Next(child);
        attempt.startTime = child->int_value; Next(child);
        attempt.result.assign(child->string_value); Next(child);
        attempt.finishTime = child->int_value; Next(child);
        attempt.attemptID.assign(child->string_value); Next(child);
        attempt.shuffleFinished = child->int_value; Next(child);
        attempt.sortFinished = child->int_value; Next(child);
        attempt.hdfsBytesRead = child->int_value; Next(child);
        attempt.hdfsBytesWritten = child->int_value; Next(child);
        attempt.fileBytesRead = child->int_value; Next(child);
        attempt.fileBytesWritten = child->int_value; Next(child);
        attempt.mapInputRecords = child->int_value; Next(child);
        attempt.mapOutputBytes = child->int_value; Next(child);
        attempt.mapOutputRecords = child->int_value; Next(child);
        attempt.combineInputRecords = child->int_value; Next(child);
        attempt.reduceInputGroups = child->int_value; Next(child);
        attempt.reduceInputRecords = child->int_value; Next(child);
        attempt.reduceShuffleBytes = child->int_value; Next(child);
        attempt.reduceOutputRecords = child->int_value; Next(child);
        attempt.spilledRecords = child->int_value; Next(child);
        attempt.mapInputBytes = child->int_value;
        attemptSet.push_back(attempt);
    }
    return attemptSet;
}

vector<Location> parseMultipleLocations(json_value *value)
{
    assert(value);
    vector<Location> locationSet;
    for(json_value *it = value->first_child; it; Next(it)) {
        Location location;
        json_value *child = it->first_child->first_child;
        location.rack.assign(child->string_value); Next(child);
        location.hostName.assign(child->string_value);
        locationSet.push_back(location);
    }
    return locationSet;
}

vector<TaskStory> parseTasks(json_value *value)
{
    assert(value);
    vector<TaskStory> taskSet;
    for(json_value *it = value->first_child; it; Next(it)) {
        TaskStory task;
        json_value *child = it->first_child;
        task.startTime = child->int_value; Next(child);
        task.attempts = parseAttempts(child); Next(child);
        task.finishTime = child->int_value; Next(child);
        if(child->first_child) task.preferredLocations = parseMultipleLocations(child); Next(child);
        task.taskType.assign(child->string_value); Next(child);
        task.taskStatus.assign(child->string_value); Next(child);
        task.taskID.assign(child->string_value); Next(child);
        task.inputBytes = child->int_value; Next(child);
        task.inputRecords = child->int_value; Next(child);
        task.outputBytes = child->int_value; Next(child);
        task.outputRecords = child->int_value;
        taskSet.push_back(task);
    }
    return taskSet;
}

vector<Ranking> parseRankings(json_value *value)
{
    assert(value);
    vector<Ranking> rankingSet;
    for(json_value *it = value->first_child; it; Next(it)) {
        Ranking rank;
        json_value *child = it->first_child;
        rank.relativeRanking = child->float_value; Next(child);
        rank.datum = child->int_value;
        rankingSet.push_back(rank);
    }
    return rankingSet;
}

vector<CDF> parseMapCDFs(json_value *value)
{
    assert(value);
    vector<CDF> cdfSet;
    for(json_value *it = value->first_child; it; Next(it)) {
        CDF cdf;
        json_value *child = it->first_child;
        cdf.maximum = child->int_value; Next(child);
        cdf.minimum = child->int_value; Next(child);
        cdf.rankings = parseRankings(child); Next(child);
        cdf.numberValues = child->int_value;
        cdfSet.push_back(cdf);
    }
    return cdfSet;
}

vector<CDF> parseReduceCDFs(json_value *value)
{
    assert(value);
    vector<CDF> cdfSet;
    CDF cdf;
    json_value *child = value->first_child;
    cdf.maximum = child->int_value; Next(child);
    cdf.minimum = child->int_value; Next(child);
    cdf.rankings = parseRankings(child); Next(child);
    cdf.numberValues = child->int_value;
    cdfSet.push_back(cdf);
    return cdfSet;
}

vector<double> parseMappers(json_value *value)
{
    assert(value);
    vector<double> mapperSet;
    for(json_value *it = value->first_child; it; Next(it)) {
        double mapper;
        mapper = it->float_value;
        mapperSet.push_back(mapper);
    }
    return mapperSet;
}

JobStory parseJobs(json_value *value)
{
    JobStory job;
    json_value *it = value->first_child;
    job.priority.assign(it->string_value); Next(it);
    job.jobID.assign(it->string_value); Next(it);
    job.user.assign(it->string_value); Next(it);
    job.jobName.assign(it->string_value); Next(it);
    job.mapTasks = parseTasks(it); Next(it);
    job.finishTime = it->int_value; Next(it);
    job.reduceTasks = parseTasks(it); Next(it);
    job.submitTime = it->int_value; Next(it);
    job.launchTime = it->int_value; Next(it);
    job.totalMaps = it->int_value; Next(it);
    job.totalReduces = it->int_value; Next(it);
    job.otherTasks = parseTasks(it); Next(it);
    job.computonsPerMapInputByte = it->int_value; Next(it);
    job.computonsPerMapOutputByte = it->int_value; Next(it);
    job.computonsPerReduceInputByte = it->int_value; Next(it);
    job.computonsPerReduceOutputByte = it->int_value; Next(it);
    job.heapMegabytes = it->int_value; Next(it);
    job.outcome.assign(it->string_value); Next(it);
    job.jobtype.assign(it->string_value); Next(it);
    assert(it->string_value == NULL); Next(it); // bypass job.directDependantJobs
    job.successfulMapAttemptCDFs = parseMapCDFs(it); Next(it);
    job.failedMapAttemptCDFs = parseMapCDFs(it); Next(it);
    job.successfulReduceAttemptCDF = parseReduceCDFs(it); Next(it);
    job.failedReduceAttemptCDF = parseReduceCDFs(it); Next(it);
    job.mapperTriesToSucceed = parseMappers(it); Next(it);
    job.failedMapperFraction = it->float_value; Next(it);
    job.relativeTime = it->int_value; Next(it);
    job.queuetype.assign(it->string_value); Next(it);
    job.clusterMapMB = it->int_value; Next(it);
    job.clusterReduceMB = it->int_value; Next(it);
    job.jobMapMB = it->int_value; Next(it);
    job.jobReduceMB = it->int_value;
    return job;
}

JobStory parseJSON(char *source)
{
	char *errorPos = 0;
	char *errorDesc = 0;
	int errorLine = 0;
	block_allocator allocator(1 << 10);

    JobStory job;
	json_value *root = json_parse(source, &errorPos, &errorDesc, &errorLine, &allocator);
	if (root) {
		job = parseJobs(root);
	} else {
	    printf("Error at line %d: %s\n%s\n\n", errorLine, errorDesc, errorPos);
	}
	return job;
}

void dumpTaskAttempt(vector<Attempt> attempts, int ident = 0)
{
    vector<Attempt>::iterator it;
    for(it = attempts.begin(); it != attempts.end(); it++) {
        IDENT(ident); cout<<"*location"<<endl;
        IDENT(ident + 1); cout<<"rack = "<<it->location.rack<<endl;
        IDENT(ident + 1); cout<<"hostName = "<<it->location.hostName<<endl;
        IDENT(ident); cout<<"*hostName = "<<it->hostName<<endl;
        IDENT(ident); cout<<"*startTime = "<<it->startTime<<endl;
        IDENT(ident); cout<<"result = "<<it->result<<endl;
        IDENT(ident); cout<<"*finishTime = "<<it->finishTime<<endl;
        IDENT(ident); cout<<"*attemptID = "<<it->attemptID<<endl;
        IDENT(ident); cout<<"shuffleFinished = "<<it->shuffleFinished<<endl;
        IDENT(ident); cout<<"sortFinished = "<<it->sortFinished<<endl;
        IDENT(ident); cout<<"hdfsBytesRead = "<<it->hdfsBytesRead<<endl;
        IDENT(ident); cout<<"hdfsBytesWritten = "<<it->hdfsBytesWritten<<endl;
        IDENT(ident); cout<<"fileBytesRead = "<<it->fileBytesRead<<endl;
        IDENT(ident); cout<<"fileBytesWritten = "<<it->fileBytesWritten<<endl;
        IDENT(ident); cout<<"mapInputRecords = "<<it->mapInputRecords<<endl;
        IDENT(ident); cout<<"mapOutputBytes = "<<it->mapOutputBytes<<endl;
        IDENT(ident); cout<<"mapOutputRecords = "<<it->mapOutputRecords<<endl;
        IDENT(ident); cout<<"combineInputRecords = "<<it->combineInputRecords<<endl;
        IDENT(ident); cout<<"reduceInputGroups = "<<it->reduceInputGroups<<endl;
        IDENT(ident); cout<<"reduceInputRecords = "<<it->reduceInputRecords<<endl;
        IDENT(ident); cout<<"reduceShuffleBytes = "<<it->reduceShuffleBytes<<endl;
        IDENT(ident); cout<<"reduceOutputRecords = "<<it->reduceOutputRecords<<endl;
        IDENT(ident); cout<<"spilledRecords = "<<it->spilledRecords<<endl;
        IDENT(ident); cout<<"mapInputBytes = "<<it->mapInputBytes<<endl;
    }
}

void dumpLocation(vector<Location> Locations, int ident = 0)
{
    vector<Location>::iterator it;
    for(it = Locations.begin(); it != Locations.end(); it++) {
        IDENT(ident); cout<<"*ip = "<<it->rack<<endl;
        IDENT(ident); cout<<"*hostName = "<<it->hostName<<endl;
    }
}

void dumpTaskStorySet(vector<TaskStory> taskSet, int ident = 0)
{
    vector<TaskStory>::iterator it;
    for(it = taskSet.begin(); it != taskSet.end(); it++) {
        IDENT(ident); cout<<"*startTime = "<<it->startTime<<endl;
        IDENT(ident); cout<<"*attempts"<<endl; dumpTaskAttempt(it->attempts, ident + 1);
        IDENT(ident); cout<<"*finishTime = "<<it->finishTime<<endl;
        IDENT(ident); cout<<"*preferredLocations"<<endl; dumpLocation(it->preferredLocations, ident + 1);
        IDENT(ident); cout<<"taskType = "<<it->taskType<<endl;
        IDENT(ident); cout<<"taskStatus = "<<it->taskStatus<<endl;
        IDENT(ident); cout<<"*taskID = "<<it->taskID<<endl;
        IDENT(ident); cout<<"inputBytes = "<<it->inputBytes<<endl;
        IDENT(ident); cout<<"inputRecords = "<<it->inputRecords<<endl;
        IDENT(ident); cout<<"outputBytes = "<<it->outputBytes<<endl;
        IDENT(ident); cout<<"outputRecords = "<<it->outputRecords<<endl;
        IDENT(ident); cout<<"======================"<<endl;
    }
}

void dumpRanking(vector<Ranking> rank, int ident = 0)
{
   vector<Ranking>::iterator it;
    for(it = rank.begin(); it != rank.end(); it++) {
        IDENT(ident); cout<<"relativeRanking = "<<it->relativeRanking<<endl;
        IDENT(ident); cout<<"datum = "<<it->datum<<endl;
        IDENT(ident); cout<<"++++++++++++++++++++"<<endl;
    }
}

void dumpCDF(vector<CDF> cdf, int ident = 0)
{
    vector<CDF>::iterator it;
    for(it = cdf.begin(); it != cdf.end(); it++) {
        IDENT(ident); cout<<"maximum = "<<it->maximum<<endl;
        IDENT(ident); cout<<"minimum = "<<it->minimum<<endl;
        IDENT(ident); cout<<"rankings"<<endl; dumpRanking(it->rankings, ident + 1);
        IDENT(ident); cout<<"numberValues = "<<it->numberValues<<endl;
        IDENT(ident); cout<<"~~~~~~~~~~~~~~~~~~~~~~~"<<endl;
    }
}

void dumpJobStory(JobStory *job, int ident)
{
    cout<<"priority = "<<job->priority<<endl;
    cout<<"*jobID = "<<job->jobID<<endl;
    cout<<"user = "<<job->user<<endl;
    cout<<"jobName = "<<job->jobName<<endl;
    cout<<"*mapTasks"<<endl; dumpTaskStorySet(job->mapTasks, ident + 1);
    cout<<"*finishTime = "<<job->finishTime<<endl;
    cout<<"*reduceTasks"<<endl; dumpTaskStorySet(job->reduceTasks, ident + 1);
    cout<<"*submitTime = "<<job->submitTime<<endl;
    cout<<"*launchTime = "<<job->launchTime<<endl;
    cout<<"*totalMaps = "<<job->totalMaps<<endl;
    cout<<"*totalReduces = "<<job->totalReduces<<endl;
    cout<<"*otherTasks"<<endl; dumpTaskStorySet(job->otherTasks, ident + 1);
    cout<<"computonsPerMapInputByte = "<<job->computonsPerMapInputByte<<endl;
    cout<<"computonsPerMapOutputByte = "<<job->computonsPerMapOutputByte<<endl;
    cout<<"computonsPerReduceInputByte = "<<job->computonsPerReduceInputByte<<endl;
    cout<<"computonsPerReduceOutputByte = "<<job->computonsPerReduceOutputByte<<endl;
    cout<<"heapMegabytes = "<<job->heapMegabytes<<endl;
    cout<<"*outcome = "<<job->outcome<<endl;
    cout<<"*jobtype = "<<job->jobtype<<endl;
    cout<<"*directDependantJobs = "<<job->directDependantJobs<<endl;
    cout<<"successfulMapAttemptCDFs"<<endl; dumpCDF(job->successfulMapAttemptCDFs, ident + 1);
    cout<<"failedMapAttemptCDFs"<<endl; dumpCDF(job->failedMapAttemptCDFs, ident + 1);
    cout<<"successfulReduceAttemptCDF"<<endl; dumpCDF(job->successfulReduceAttemptCDF, ident + 1);
    cout<<"failedReduceAttemptCDF"<<endl; dumpCDF(job->failedReduceAttemptCDF, ident + 1);
    cout<<"mapperTriesToSucceed"<<endl;
    for(size_t j = 0; j < job->mapperTriesToSucceed.size(); j++) {
        IDENT(ident + 1); cout<<job->mapperTriesToSucceed[j]<<endl;
    }
    cout<<"failedMapperFraction = "<<job->failedMapperFraction<<endl;
    cout<<"relativeTime = "<<job->relativeTime<<endl;
    cout<<"queuetype = "<<job->queuetype<<endl;
    cout<<"clusterMapMB = "<<job->clusterMapMB<<endl;
    cout<<"clusterReduceMB = "<<job->clusterReduceMB<<endl;
    cout<<"jobMapMB = "<<job->jobMapMB<<endl;
    cout<<"jobReduceMB = "<<job->jobReduceMB<<endl;
}

void dumpJobStorySet(int ident = 0)
{
     deque<JobStory>::iterator it;
     for(it = jobSet.begin(); it != jobSet.end(); it++) {
        cout<<"priority = "<<it->priority<<endl;
        cout<<"*jobID = "<<it->jobID<<endl;
        cout<<"user = "<<it->user<<endl;
        cout<<"jobName = "<<it->jobName<<endl;
        cout<<"*mapTasks"<<endl; dumpTaskStorySet(it->mapTasks, ident + 1);
        cout<<"*finishTime = "<<it->finishTime<<endl;
        cout<<"*reduceTasks"<<endl; dumpTaskStorySet(it->reduceTasks, ident + 1);
        cout<<"*submitTime = "<<it->submitTime<<endl;
        cout<<"*launchTime = "<<it->launchTime<<endl;
        cout<<"*totalMaps = "<<it->totalMaps<<endl;
        cout<<"*totalReduces = "<<it->totalReduces<<endl;
        cout<<"*otherTasks"<<endl; dumpTaskStorySet(it->otherTasks, ident + 1);
        cout<<"computonsPerMapInputByte = "<<it->computonsPerMapInputByte<<endl;
        cout<<"computonsPerMapOutputByte = "<<it->computonsPerMapOutputByte<<endl;
        cout<<"computonsPerReduceInputByte = "<<it->computonsPerReduceInputByte<<endl;
        cout<<"computonsPerReduceOutputByte = "<<it->computonsPerReduceOutputByte<<endl;
        cout<<"heapMegabytes = "<<it->heapMegabytes<<endl;
        cout<<"*outcome = "<<it->outcome<<endl;
        cout<<"*jobtype = "<<it->jobtype<<endl;
        cout<<"*directDependantJobs = "<<it->directDependantJobs<<endl;
        cout<<"successfulMapAttemptCDFs"<<endl; dumpCDF(it->successfulMapAttemptCDFs, ident + 1);
        cout<<"failedMapAttemptCDFs"<<endl; dumpCDF(it->failedMapAttemptCDFs, ident + 1);
        cout<<"successfulReduceAttemptCDF"<<endl; dumpCDF(it->successfulReduceAttemptCDF, ident + 1);
        cout<<"failedReduceAttemptCDF"<<endl; dumpCDF(it->failedReduceAttemptCDF, ident + 1);
        cout<<"mapperTriesToSucceed"<<endl;
        for(size_t j = 0; j < it->mapperTriesToSucceed.size(); j++) {
            IDENT(ident + 1); cout<<it->mapperTriesToSucceed[j]<<endl;
        }
        cout<<"failedMapperFraction = "<<it->failedMapperFraction<<endl;
        cout<<"relativeTime = "<<it->relativeTime<<endl;
        cout<<"queuetype = "<<it->queuetype<<endl;
        cout<<"clusterMapMB = "<<it->clusterMapMB<<endl;
        cout<<"clusterReduceMB = "<<it->clusterReduceMB<<endl;
        cout<<"jobMapMB = "<<it->jobMapMB<<endl;
        cout<<"jobReduceMB = "<<it->jobReduceMB<<endl;
        IDENT(ident); cout<<"------------------------------"<<endl;
    }
}

void initTraceReader(string traceFilePrefix, int numTraceFiles, bool debug)
{
    vector<vector<char> > traces;
    int nameLength = traceFilePrefix.size();

    // Load all the trace files
    for(int i = 0; i < numTraceFiles; i++) {
        traceFilePrefix.append(to_string(i));
		FILE *fp = fopen(traceFilePrefix.c_str(), "rb");
		if (fp) {
			fseek(fp, 0, SEEK_END);
			long size = ftell(fp);
			fseek(fp, 0, SEEK_SET);
			vector<char> buffer(size + 1);
			size_t result = fread(&buffer[0], 1, size, fp);
			assert(result == (size_t)size);
			fclose(fp);
			traces.push_back(buffer);
		}
		else {
            cout<<"Trace file '"<<traceFilePrefix<<"' loaded error.\n";
		}
        traceFilePrefix.resize(nameLength);
    }

    // Parse JSON format and store into Job/Task Story structure
    for (size_t i = 0; i < traces.size(); i++) {
        JobStory job = parseJSON(&traces[i][0]);
        jobSet.push_back(job);
	}
    traces.clear();

    if (debug) {
        // verify the jobset
        //dumpJobStorySet();
        startAnalysis(true);
    }
}

bool isAllJobsDone()
{
    return jobSet.empty() && runningJobSet.empty();
}

bool isMoreJobs()
{
    return (!jobSet.empty());
}

long nextJobSubmitTime()
{
    assert(!jobSet.empty());
    JobStory job = jobSet.front();
    return job.submitTime;
}

JobStory fetchNextJob()
{
    JobStory job = jobSet.front();
    jobSet.pop_front();
    runningJobSet.push_back(job);
    return job;
}

void completeJob(string jobID)
{
    deque<JobStory>::iterator it;
    for(it = runningJobSet.begin(); it != runningJobSet.end(); it++) {
        if(it->jobID == jobID)
            break;
    }
    assert(it != runningJobSet.end());
    completedJobSet.push_back(*it);
    runningJobSet.erase(it);
}

deque<JobStory> getAllJobs()
{
    return jobSet;
}
