#include <assert.h>
#include <stdio.h>
#include <iostream>
#include <fstream>
#include "TraceAnalyzer.h"
#include "TraceReader.h"
#include "json/json.h"
using namespace std;

/* TraceReader Variables */
static deque<JobStory> jobSet;
static deque<JobStory> runningJobSet;
static deque<JobStory> completedJobSet;

#define IDENT(n) for (int i = 0; i < n; ++i) jsonFile<<"  "
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
        attempt.attemptID.assign(child->string_value); Next(child);
        attempt.combineInputRecords = child->int_value; Next(child);
        attempt.fileBytesRead = child->int_value; Next(child);
        attempt.fileBytesWritten = child->int_value; Next(child);
        attempt.finishTime = child->int_value; Next(child);
        attempt.hdfsBytesRead = child->int_value; Next(child);
        attempt.hdfsBytesWritten = child->int_value; Next(child);
        if (child->string_value) attempt.hostName.assign(child->string_value); Next(child);
        if (child->first_child) attempt.location = parseSingleLocation(child); Next(child);
        attempt.mapInputBytes = child->int_value; Next(child);
        attempt.mapInputRecords = child->int_value; Next(child);
        attempt.mapOutputBytes = child->int_value; Next(child);
        attempt.mapOutputRecords = child->int_value; Next(child);
        attempt.reduceInputGroups = child->int_value; Next(child);
        attempt.reduceInputRecords = child->int_value; Next(child);
        attempt.reduceOutputRecords = child->int_value; Next(child);
        attempt.reduceShuffleBytes = child->int_value; Next(child);
        attempt.result.assign(child->string_value); Next(child);
        attempt.shuffleFinished = child->int_value; Next(child);
        attempt.sortFinished = child->int_value; Next(child);
        attempt.spilledRecords = child->int_value; Next(child);
        attempt.startTime = child->int_value;
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
        task.attempts = parseAttempts(child); Next(child);
        task.finishTime = child->int_value; Next(child);
        task.inputBytes = child->int_value; Next(child);
        task.inputRecords = child->int_value; Next(child);
        task.outputBytes = child->int_value; Next(child);
        task.outputRecords = child->int_value; Next(child);
        if (child->first_child) task.preferredLocations = parseMultipleLocations(child); Next(child);
        task.startTime = child->int_value; Next(child);
        task.taskID.assign(child->string_value); Next(child);
        task.taskStatus.assign(child->string_value); Next(child);
        task.taskType.assign(child->string_value);
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
        rank.datum = child->int_value; Next(child);
        rank.relativeRanking = child->float_value;
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
        cdf.numberValues = child->int_value; Next(child);
        cdf.rankings = parseRankings(child);
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
    cdf.numberValues = child->int_value; Next(child);
    cdf.rankings = parseRankings(child);
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
    job.clusterMapMB = it->int_value; Next(it);
    job.clusterReduceMB = it->int_value; Next(it);
    job.computonsPerMapInputByte = it->int_value; Next(it);
    job.computonsPerMapOutputByte = it->int_value; Next(it);
    job.computonsPerReduceInputByte = it->int_value; Next(it);
    job.computonsPerReduceOutputByte = it->int_value; Next(it);
    assert(it->string_value == NULL); Next(it); // bypass job.directDependantJobs
    job.failedMapAttemptCDFs = parseMapCDFs(it); Next(it);
    job.failedMapperFraction = it->float_value; Next(it);
    job.failedReduceAttemptCDF = parseReduceCDFs(it); Next(it);
    job.finishTime = it->int_value; Next(it);
    job.heapMegabytes = it->int_value; Next(it);
    job.jobID.assign(it->string_value); Next(it);
    job.jobMapMB = it->int_value; Next(it);
    job.jobName.assign(it->string_value); Next(it);
    job.jobReduceMB = it->int_value; Next(it);
    job.jobtype.assign(it->string_value); Next(it);
    job.launchTime = it->int_value; Next(it);
    job.mapTasks = parseTasks(it); Next(it);
    job.mapperTriesToSucceed = parseMappers(it); Next(it);
    job.otherTasks = parseTasks(it); Next(it);
    job.outcome.assign(it->string_value); Next(it);
    job.priority.assign(it->string_value); Next(it);
    job.queuetype.assign(it->string_value); Next(it);
    job.reduceTasks = parseTasks(it); Next(it);
    job.relativeTime = it->int_value; Next(it);
    job.submitTime = it->int_value; Next(it);
    job.successfulMapAttemptCDFs = parseMapCDFs(it); Next(it);
    job.successfulReduceAttemptCDF = parseReduceCDFs(it); Next(it);
    job.totalMaps = it->int_value; Next(it);
    job.totalReduces = it->int_value; Next(it);
    job.user.assign(it->string_value);
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

void dumpTaskAttempt(ofstream &jsonFile, vector<Attempt> &attempts, int ident)
{
    for(size_t i = 0; i < attempts.size(); i++) {
        IDENT(ident); jsonFile<<"{"<<endl;
        IDENT(ident+1); jsonFile<<"\"attemptID\" : \""<<attempts[i].attemptID<<"\","<<endl;
        IDENT(ident+1); jsonFile<<"\"combineInputRecords\" : "<<attempts[i].combineInputRecords<<","<<endl;
        IDENT(ident+1); jsonFile<<"\"fileBytesRead\" : "<<attempts[i].fileBytesRead<<","<<endl;
        IDENT(ident+1); jsonFile<<"\"fileBytesWritten\" : "<<attempts[i].fileBytesWritten<<","<<endl;
        IDENT(ident+1); jsonFile<<"\"finishTime\" : "<<attempts[i].finishTime<<","<<endl;
        IDENT(ident+1); jsonFile<<"\"hdfsBytesRead\" : "<<attempts[i].hdfsBytesRead<<","<<endl;
        IDENT(ident+1); jsonFile<<"\"hdfsBytesWritten\" : "<<attempts[i].hdfsBytesWritten<<","<<endl;
        if (attempts[i].hostName.empty()) {
            IDENT(ident+1); jsonFile<<"\"hostName\" : null,"<<endl;
        } else {
            IDENT(ident+1); jsonFile<<"\"hostName\" : \""<<attempts[i].hostName<<"\","<<endl;
        }

        if (attempts[i].location.rack.empty() && attempts[i].location.hostName.empty()) {
            IDENT(ident+1); jsonFile<<"\"location\" : null,"<<endl;
        } else {
            IDENT(ident+1); jsonFile<<"\"location\" : {"<<endl;
            IDENT(ident+2); jsonFile<<"\"layers\" : ["<<endl;
            IDENT(ident+3); jsonFile<<"\""<<attempts[i].location.rack<<"\","<<endl;
            IDENT(ident+3); jsonFile<<"\""<<attempts[i].location.hostName<<"\""<<endl;
            IDENT(ident+2); jsonFile<<"]"<<endl;
            IDENT(ident+1); jsonFile<<"},"<<endl;
        }
        IDENT(ident+1); jsonFile<<"\"mapInputBytes\" : "<<attempts[i].mapInputBytes<<","<<endl;
        IDENT(ident+1); jsonFile<<"\"mapInputRecords\" : "<<attempts[i].mapInputRecords<<","<<endl;
        IDENT(ident+1); jsonFile<<"\"mapOutputBytes\" : "<<attempts[i].mapOutputBytes<<","<<endl;
        IDENT(ident+1); jsonFile<<"\"mapOutputRecords\" : "<<attempts[i].mapOutputRecords<<","<<endl;
        IDENT(ident+1); jsonFile<<"\"reduceInputGroups\" : "<<attempts[i].reduceInputGroups<<","<<endl;
        IDENT(ident+1); jsonFile<<"\"reduceInputRecords\" : "<<attempts[i].reduceInputRecords<<","<<endl;
        IDENT(ident+1); jsonFile<<"\"reduceOutputRecords\" : "<<attempts[i].reduceOutputRecords<<","<<endl;
        IDENT(ident+1); jsonFile<<"\"reduceShuffleBytes\" : "<<attempts[i].reduceShuffleBytes<<","<<endl;
        IDENT(ident+1); jsonFile<<"\"result\" : \""<<attempts[i].result<<"\","<<endl;
        IDENT(ident+1); jsonFile<<"\"shuffleFinished\" : "<<attempts[i].shuffleFinished<<","<<endl;
        IDENT(ident+1); jsonFile<<"\"sortFinished\" : "<<attempts[i].sortFinished<<","<<endl;
        IDENT(ident+1); jsonFile<<"\"spilledRecords\" : "<<attempts[i].spilledRecords<<","<<endl;
        IDENT(ident+1); jsonFile<<"\"startTime\" : "<<attempts[i].startTime<<endl;
        IDENT(ident); jsonFile<<"}";
        if (i != attempts.size() - 1)
            jsonFile<<","<<endl;
        else
            jsonFile<<endl;
    }
}

void dumpLocation(ofstream &jsonFile, vector<Location> &locations, int ident)
{
    for(size_t i = 0; i < locations.size(); i++) {
        IDENT(ident); jsonFile<<"{"<<endl;
        IDENT(ident+1); jsonFile<<"\"layers\" : ["<<endl;
        IDENT(ident+2); jsonFile<<"\""<<locations[i].rack<<"\","<<endl;
        IDENT(ident+2); jsonFile<<"\""<<locations[i].hostName<<"\""<<endl;
        IDENT(ident+1); jsonFile<<"]"<<endl;
        IDENT(ident); jsonFile<<"}";
        if (i != locations.size() - 1)
            jsonFile<<","<<endl;
        else
            jsonFile<<endl;
    }
}

void dumpTaskStorySet(ofstream &jsonFile, vector<TaskStory> &taskSet, int ident)
{
    for(size_t i = 0; i < taskSet.size(); i++) {
        IDENT(ident); jsonFile<<"{"<<endl;
        if (taskSet[i].attempts.empty()) {
            IDENT(ident+1); jsonFile<<"\"attempts\" : [],"<<endl;
        } else {
            IDENT(ident+1); jsonFile<<"\"attempts\" : ["<<endl;
            dumpTaskAttempt(jsonFile, taskSet[i].attempts, ident+2);
            IDENT(ident+1); jsonFile<<"],"<<endl;
        }
        IDENT(ident+1); jsonFile<<"\"finishTime\" : "<<taskSet[i].finishTime<<","<<endl;
        IDENT(ident+1); jsonFile<<"\"inputBytes\" : "<<taskSet[i].inputBytes<<","<<endl;
        IDENT(ident+1); jsonFile<<"\"inputRecords\" : "<<taskSet[i].inputRecords<<","<<endl;
        IDENT(ident+1); jsonFile<<"\"outputBytes\" : "<<taskSet[i].outputBytes<<","<<endl;
        IDENT(ident+1); jsonFile<<"\"outputRecords\" : "<<taskSet[i].outputRecords<<","<<endl;
        if (taskSet[i].preferredLocations.empty()) {
            IDENT(ident+1); jsonFile<<"\"preferredLocations\" : [],"<<endl;
        } else {
            IDENT(ident+1); jsonFile<<"\"preferredLocations\" : ["<<endl;
            dumpLocation(jsonFile, taskSet[i].preferredLocations, ident+2);
            IDENT(ident+1); jsonFile<<"],"<<endl;
        }
        IDENT(ident+1); jsonFile<<"\"startTime\" : "<<taskSet[i].startTime<<","<<endl;
        IDENT(ident+1); jsonFile<<"\"taskID\" : \""<<taskSet[i].taskID<<"\","<<endl;
        IDENT(ident+1); jsonFile<<"\"taskStatus\" : \""<<taskSet[i].taskStatus<<"\","<<endl;
        IDENT(ident+1); jsonFile<<"\"taskType\" : \""<<taskSet[i].taskType<<"\""<<endl;
        IDENT(ident); jsonFile<<"}";
        if (i != taskSet.size() - 1)
            jsonFile<<","<<endl;
        else
            jsonFile<<endl;
    }
}

void dumpRanking(ofstream &jsonFile, vector<Ranking> &rank, int ident)
{
    for(size_t i = 0; i < rank.size(); i++) {
        IDENT(ident); jsonFile<<"{"<<endl;
        IDENT(ident+1); jsonFile<<"\"datum\" : "<<rank[i].datum<<","<<endl;
        IDENT(ident+1); jsonFile<<"\"relativeRanking\" : "<<rank[i].relativeRanking<<endl;
        IDENT(ident); jsonFile<<"}";
        if (i != rank.size() - 1)
            jsonFile<<","<<endl;
        else
            jsonFile<<endl;
    }
}

void dumpCDF(ofstream &jsonFile, vector<CDF> &cdf, int ident, bool bracket)
{
    for(size_t i = 0; i < cdf.size(); i++) {
        if (bracket) {
            IDENT(ident); jsonFile<<"{"<<endl;
        }
        IDENT(ident+1); jsonFile<<"\"maximum\" : "<<cdf[i].maximum<<","<<endl;
        IDENT(ident+1); jsonFile<<"\"minimum\" : "<<cdf[i].minimum<<","<<endl;
        IDENT(ident+1); jsonFile<<"\"numberValues\" : "<<cdf[i].numberValues<<","<<endl;
        if (cdf[i].rankings.empty()) {
            IDENT(ident+1); jsonFile<<"\"rankings\" : []"<<endl;
        } else {
            IDENT(ident+1); jsonFile<<"\"rankings\" : ["<<endl;
            dumpRanking(jsonFile, cdf[i].rankings, ident+2);
            IDENT(ident+1); jsonFile<<"]"<<endl;
        }
        if (bracket) {
            IDENT(ident); jsonFile<<"}";
            if (i != cdf.size() - 1)
                jsonFile<<","<<endl;
            else
                jsonFile<<endl;
        }
    }
}

void dumpJobStory(ofstream &jsonFile, JobStory &job, int ident = 1)
{
    jsonFile<<"{"<<endl;
    IDENT(ident); jsonFile<<"\"clusterMapMB\" : "<<job.clusterMapMB<<","<<endl;
    IDENT(ident); jsonFile<<"\"clusterReduceMB\" : "<<job.clusterReduceMB<<","<<endl;
    IDENT(ident); jsonFile<<"\"computonsPerMapInputByte\" : "<<job.computonsPerMapInputByte<<","<<endl;
    IDENT(ident); jsonFile<<"\"computonsPerMapOutputByte\" : "<<job.computonsPerMapOutputByte<<","<<endl;
    IDENT(ident); jsonFile<<"\"computonsPerReduceInputByte\" : "<<job.computonsPerReduceInputByte<<","<<endl;
    IDENT(ident); jsonFile<<"\"computonsPerReduceOutputByte\" : "<<job.computonsPerReduceOutputByte<<","<<endl;
    IDENT(ident); jsonFile<<"\"directDependantJobs\" : [],"<<endl;

    if (job.failedMapAttemptCDFs.empty()) {
        IDENT(ident); jsonFile<<"\"failedMapAttemptCDFs\" : [],"<<endl;
    } else {
        IDENT(ident); jsonFile<<"\"failedMapAttemptCDFs\" : ["<<endl;
        dumpCDF(jsonFile, job.failedMapAttemptCDFs, ident+1, true);
        IDENT(ident); jsonFile<<"],"<<endl;
    }
    IDENT(ident); jsonFile<<"\"failedMapperFraction\" : "<<job.failedMapperFraction<<","<<endl;

    assert(job.failedReduceAttemptCDF.size() == 1);
    IDENT(ident); jsonFile<<"\"failedReduceAttemptCDF\" : {"<<endl;
    dumpCDF(jsonFile, job.failedReduceAttemptCDF, ident, false);
    IDENT(ident); jsonFile<<"},"<<endl;

    IDENT(ident); jsonFile<<"\"finishTime\" : "<<job.finishTime<<","<<endl;
    IDENT(ident); jsonFile<<"\"heapMegabytes\" : "<<job.heapMegabytes<<","<<endl;
    IDENT(ident); jsonFile<<"\"jobID\" : \""<<job.jobID<<"\","<<endl;
    IDENT(ident); jsonFile<<"\"jobMapMB\" : "<<job.jobMapMB<<","<<endl;
    IDENT(ident); jsonFile<<"\"jobName\" : \""<<job.jobName<<"\","<<endl;
    IDENT(ident); jsonFile<<"\"jobReduceMB\" : "<<job.jobReduceMB<<","<<endl;
    IDENT(ident); jsonFile<<"\"jobtype\" : \""<<job.jobtype<<"\","<<endl;
    IDENT(ident); jsonFile<<"\"launchTime\" : "<<job.launchTime<<","<<endl;

    if (job.mapTasks.empty()) {
        IDENT(ident); jsonFile<<"\"mapTasks\" : [],"<<endl;
    } else {
        IDENT(ident); jsonFile<<"\"mapTasks\" : ["<<endl;
        dumpTaskStorySet(jsonFile, job.mapTasks, ident+1);
        IDENT(ident); jsonFile<<"],"<<endl;
    }

    if (job.mapperTriesToSucceed.empty()) {
        IDENT(ident); jsonFile<<"\"mapperTriesToSucceed\" : [],"<<endl;
    } else {
        IDENT(ident); jsonFile<<"\"mapperTriesToSucceed\" : ["<<endl;
        for(size_t i = 0; i < job.mapperTriesToSucceed.size(); i++) {
            IDENT(ident+1); jsonFile<<job.mapperTriesToSucceed[i];
            if (i != job.mapperTriesToSucceed.size() - 1)
                jsonFile<<","<<endl;
            else
                jsonFile<<endl;
        }
        IDENT(ident); jsonFile<<"],"<<endl;
    }

    if (job.otherTasks.empty()) {
        IDENT(ident); jsonFile<<"\"otherTasks\" : [],"<<endl;
    } else {
        IDENT(ident); jsonFile<<"\"otherTasks\" : ["<<endl;
        dumpTaskStorySet(jsonFile, job.otherTasks, ident+1);
        IDENT(ident); jsonFile<<"],"<<endl;
    }

    IDENT(ident); jsonFile<<"\"outcome\" : \""<<job.outcome<<"\","<<endl;
    IDENT(ident); jsonFile<<"\"priority\" : \""<<job.priority<<"\","<<endl;
    IDENT(ident); jsonFile<<"\"queue\" : \""<<job.queuetype<<"\","<<endl;

    if (job.reduceTasks.empty()) {
        IDENT(ident); jsonFile<<"\"reduceTasks\" : [],"<<endl;
    } else {
        IDENT(ident); jsonFile<<"\"reduceTasks\" : ["<<endl;
        dumpTaskStorySet(jsonFile, job.reduceTasks, ident+1);
        IDENT(ident); jsonFile<<"],"<<endl;
    }
    IDENT(ident); jsonFile<<"\"relativeTime\" : "<<job.relativeTime<<","<<endl;
    IDENT(ident); jsonFile<<"\"submitTime\" : "<<job.submitTime<<","<<endl;

    if (job.successfulMapAttemptCDFs.empty()) {
        IDENT(ident); jsonFile<<"\"successfulMapAttemptCDFs\" : [],"<<endl;
    } else {
        IDENT(ident); jsonFile<<"\"successfulMapAttemptCDFs\" : ["<<endl;
        dumpCDF(jsonFile, job.successfulMapAttemptCDFs, ident+1, true);
        IDENT(ident); jsonFile<<"],"<<endl;
    }

    assert(job.successfulReduceAttemptCDF.size() == 1);
    IDENT(ident); jsonFile<<"\"successfulReduceAttemptCDF\" : {"<<endl;
    dumpCDF(jsonFile, job.successfulReduceAttemptCDF, ident, false);
    IDENT(ident); jsonFile<<"},"<<endl;

    IDENT(ident); jsonFile<<"\"totalMaps\" : "<<job.totalMaps<<","<<endl;
    IDENT(ident); jsonFile<<"\"totalReduces\" : "<<job.totalReduces<<","<<endl;
    IDENT(ident); jsonFile<<"\"user\" : \""<<job.user<<"\""<<endl;
    jsonFile<<"}";
}

void dumpJobStory(JobStory &job, string debugDir)
{
    ofstream jsonFile;
    jsonFile.open((debugDir + job.jobID + "_Runtime.json").c_str());
    dumpJobStory(jsonFile, job);
    jsonFile.close();
}

void dumpJobStorySet(string debugDir)
{
    ofstream jsonFile;
    deque<JobStory>::iterator it;
    int i;
    for(i = 0, it = jobSet.begin(); it != jobSet.end(); i++, it++) {
        jsonFile.open((debugDir + it->jobID + ".json").c_str());
        dumpJobStory(jsonFile, *it);
        jsonFile.close();
    }
}

void initTraceReader(string traceFilePrefix, int numTraceFiles, bool debug, string debugDir)
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
        dumpJobStorySet(debugDir);
        startAnalysis(true, debugDir);
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
