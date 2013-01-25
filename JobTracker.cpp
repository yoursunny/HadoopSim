#include <assert.h>
#include <math.h>
#include <stdio.h>
#include <iostream>
#include "EventQueue.h"
#include "JobClient.h"
#include "JobTracker.h"
#include "TraceReader.h"
using namespace HadoopNetSim;
using namespace std;

/* JobTracker Variables */
static FILE *netoptFile = NULL;     // traffic log file
static JobTracker *jobTracker;
static FIFOScheduler fifoSched("FIFOScheduler", 0);
static DataLocalityScheduler dataLocalitySched("DataLocalityScheduler", 1);
static const long clusterStartupDuration = 100 * 1000;  //milliseconds
static const long SNMP_INTERVAL_MIN = 3 * 1000;         //milliseconds
static const int CLUSTER_INCREMENT = 100;
static long nextSNMPInterval = 0;
static const size_t LinkStatQueueLen = 20;
static long scaledMapCPUTime; //ms
static long scaledDownRatioForReduce;
static long customMapNum;
static long customReduceNum;
static long blockPerTT = 0;

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

static unsigned long rackIndex = 0;
static unordered_map<string, unsigned long> nodeIndexPerRack;
void JobTracker::handleBlockPlacement(string splitID)
{
    unordered_map<RackName, vector<HostName>> rackSet = getRackSet();
    size_t rackNum = rackSet.size();
    vector<string> dataNodes;

    while(1) {
        string rackName = "rack" + to_string(rackIndex % rackNum);
        vector<HostName> tt = rackSet[rackName];
        assert(!tt.empty());
        size_t i, j, k;
        for(k = 0, i = nodeIndexPerRack[rackName]; k < tt.size(); k++, i++) {
            if(tt[i%tt.size()] == "manager") continue;
            if(nodeBlockCnt[tt[i%tt.size()]] < blockPerTT)
                break;
        }
        if (k == tt.size()) {
            rackIndex++;    // all tt(s) in current rack are full, try next rack
            continue;
        }
        nodeIndexPerRack[rackName]++;

        if(dataNodes.size() == 2) {
            dataNodes.push_back(tt[i%tt.size()]);
            nodeBlockCnt[tt[i%tt.size()]]++;
            break;
        }

        for(k = 0, j = nodeIndexPerRack[rackName]; k < tt.size(); k++, j++) {
            if (i == j) continue;
            if(tt[j%tt.size()] == "manager") continue;
            if(nodeBlockCnt[tt[j%tt.size()]] < blockPerTT)
                break;
        }
        if (k == tt.size()) {
            rackIndex++;
            continue;
        }
        nodeIndexPerRack[rackName]++;

        dataNodes.push_back(tt[i%tt.size()]); nodeBlockCnt[tt[i%tt.size()]]++;
        dataNodes.push_back(tt[j%tt.size()]); nodeBlockCnt[tt[j%tt.size()]]++;
        rackIndex++;
    }
    assert(dataNodes.size() == 3);
    rackIndex++;
    updateBlockNodeMapping(splitID, dataNodes);
}

// accept the new job and create runtime Job data structure.
void JobTracker::acceptNewJob(JobStory *jobStory, long now)
{
    Job job;
    if (customMapNum <= jobStory->totalMaps && customReduceNum <= jobStory->totalReduces) {
        job = Job(jobStory->jobID, jobStory->totalMaps, jobStory->totalReduces, now);
        blockPerTT += (int)ceil(jobStory->totalMaps * 3.0 / getClusterSlaveNodes().size());
    } else if (customMapNum > jobStory->totalMaps && customReduceNum > jobStory->totalReduces) {
        job = Job(jobStory->jobID, customMapNum, customReduceNum, now);
        blockPerTT += (int)ceil(customMapNum * 3.0 / getClusterSlaveNodes().size());
    } else {
        cout<<"Error custom task numbers.\n";
        assert(0);
    }
    job.initMapTasks(jobStory->mapTasks, scaledMapCPUTime, customMapNum);
    job.initReduceTasks(jobStory->reduceTasks, scaledDownRatioForReduce, customReduceNum);
    runningJobs.insert(pair<string, Job>(jobStory->jobID, job));

    int totalBlockNum = 0;
    for(map<string, long>::iterator it = nodeBlockCnt.begin(); it != nodeBlockCnt.end(); ++it) {
        cout<<it->first<<" : "<<it->second<<endl;
        totalBlockNum += it->second;
    }
    cout<<"totalBlockNum="<<totalBlockNum<<endl;
}

void JobTracker::updateTaskTrackerStatus(HeartBeatReport report, long now)
{
    vector<TaskStatus> runningMaps;
    vector<TaskStatus> runningReduces;

    list<TaskStatus> taskStatus = report.taskStatus;
    for(list<TaskStatus>::iterator taskStatusIt = taskStatus.begin(); taskStatusIt != taskStatus.end(); taskStatusIt++) {
        if (taskStatusIt->runState != RUNNING)
            continue;
        if (taskStatusIt->type == MAPTASK)
            runningMaps.push_back(*taskStatusIt);
        if (taskStatusIt->type == REDUCETASK)
            runningReduces.push_back(*taskStatusIt);
    }

    map<string, TaskTrackerStatus>::iterator it;
    it = allTaskTrackerStatus.find(report.hostName);
    if (it != allTaskTrackerStatus.end()) {
        TaskTrackerStatus trackerStatus = it->second;
        assert(trackerStatus.getLastReportTime() < now);
        trackerStatus.setLastReportTime(now);
        trackerStatus.setAvailMapSlots(report.numAvailMapSlots);
        trackerStatus.setAvailReduceSlots(report.numAvailReduceSlots);
        trackerStatus.setRunningMaps(runningMaps);
        trackerStatus.setRunningReduces(runningReduces);
        it->second = trackerStatus;
    } else {
        // initial HeartBeat report from the TaskTracker
        TaskTrackerStatus trackerStatus(report.hostName);
        trackerStatus.setLastReportTime(now);
        trackerStatus.setAvailMapSlots(report.numAvailMapSlots);
        trackerStatus.setAvailReduceSlots(report.numAvailReduceSlots);
        trackerStatus.setRunningMaps(runningMaps);
        trackerStatus.setRunningReduces(runningReduces);
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

            cout<<"xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx Job "<<jobIt->first<<" done\n";
            // also update TraceReader completedJobSet
            completeJob(jobIt->first);

            // ask for a new job
            JobClient *client = getJobClient();
            HEvent evt(client, EVT_JobDone);
            ns3::Simulator::Schedule(ns3::Seconds(1.0), &hadoopEventCallback, evt);
        }
    }
}

TaskTrackerStatus JobTracker::getTaskTrackerStatus(std::string trackerName)
{
    assert(allTaskTrackerStatus.find(trackerName) != allTaskTrackerStatus.end());
    return allTaskTrackerStatus.find(trackerName)->second;
}

vector<MapDataAction> JobTracker::issueMapDataAction(string taskTrackerName)        // todo
{
    vector<MapDataAction> mapDataActions;
    map<string, vector<MapDataAction>>::iterator dataActionIt = allMapDataActions.find(taskTrackerName);
    if (dataActionIt != allMapDataActions.end()) {
        vector<MapDataAction> actions = dataActionIt->second;
        mapDataActions = actions;
        actions.clear();
        dataActionIt->second = actions;
    }

    return mapDataActions;
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

    if (sched->getSchedID() == 0) { // defaul FIFO scheduler Policy
        // combine MapDataAction from two different cases, and decide which MapDataAction to be issued to this tasktracker
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
    } else if (sched->getSchedID() == 1) {  // new scheduler
        if (allMapDataActions.find(report.hostName) != allMapDataActions.end()) {
            vector<MapDataAction> actionBufffer = allMapDataActions[report.hostName];
            while(!mapDataActions.empty()) {
                actionBufffer.push_back(mapDataActions.back());
                mapDataActions.pop_back();
            }
            allMapDataActions[report.hostName] = actionBufffer;
        } else {
            allMapDataActions[report.hostName] = mapDataActions;
        }

        response.mapDataActions = issueMapDataAction(report.hostName);
    }
    else {
        cout<<"Unknown Scheduler.\n";
        assert(0);
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

const size_t JobTracker::getTaskTrackerCount(void) const
{
    return getClusterSlaveNodes().size();
}

const long JobTracker::getMapSlotCapacity(void) const
{
    return MaxMapSlots * this->getTaskTrackerCount();
}

void JobTracker::sendSNMPMsg(string switchName)
{
    NetSim *netsim = getNetSim();
    assert(netsim != NULL);

    vector<LinkId> links = getNodeOutLinks(switchName);
    vector<LinkInfo> linkInfo;
    for(size_t i = 0; i < links.size(); ++i) {
        ns3::Ptr<LinkStat> stat = netsim->GetLinkStat(links[i]);
        LinkInfo info;
        info.linkID = stat->id();
        info.bandwidth = stat->bandwidth_utilization(linkStats.at(info.linkID));
        info.queue = stat->queue_utilization();
        linkInfo.push_back(info);
        linkStats[info.linkID] = stat;
    }

    MsgId id;
    id = netsim->Snmp(switchName, this->hostName, links.size() * sizeof(LinkInfo), ns3::MakeCallback(&JobTracker::receiveSNMPMsg, this), this);
    assert(id != MsgId_invalid);

    assert(snmpMsg.find(id) == snmpMsg.end());
    snmpMsg[id] = linkInfo;

    // add next SNMP event from this switch to the EventQueue
    ns3::Simulator::Schedule(ns3::Seconds((double)nextSNMPInterval/1000.0), &JobTracker::sendSNMPMsg, this, switchName);
}

void JobTracker::receiveSNMPMsg(ns3::Ptr<MsgInfo> msg)
{
    assert(msg->type() == kMTSnmp);
    assert(snmpMsg.find(msg->id()) != snmpMsg.end());

    // update Network Information Database in JotTracker
    vector<LinkInfo> linkInfo = snmpMsg[msg->id()];
    unordered_map<LinkId, queue<LinkInfo>> linkInfoMap;
    queue<LinkInfo> linkInfoQueue;
    for(size_t i = 0; i < linkInfo.size(); ++i) {
        linkInfoMap.clear();
        while(!linkInfoQueue.empty())
            linkInfoQueue.pop();
        if (nodeLinkStat.find(msg->src()) != nodeLinkStat.end()) {
            linkInfoMap = nodeLinkStat[msg->src()];
            if (linkInfoMap.find(linkInfo[i].linkID) != linkInfoMap.end()) {
                linkInfoQueue = linkInfoMap[linkInfo[i].linkID];
            }
        }
        while(linkInfoQueue.size() >= LinkStatQueueLen) {
            linkInfoQueue.pop();
        }
        linkInfoQueue.push(linkInfo[i]);
        linkInfoMap[linkInfo[i].linkID] = linkInfoQueue;
        nodeLinkStat[msg->src()] = linkInfoMap;
    }

    // debug only
    /*
    cout<<"SNMP MSG Received at "<<ns3::Simulator::Now().GetMilliSeconds()<<endl;
    cout<<"Switch: "<<msg->src()<<endl;
    for(size_t i = 0; i < linkInfo.size(); ++i) {
        cout<<"\tlinkID = "<<linkInfo[i].linkID<<", bandwidth = "<<linkInfo[i].bandwidth<<", queue = "<<linkInfo[i].queue<<endl;
    }
    cout<<"SNMP MSG End\n\n";
    */
    snmpMsg.erase(msg->id());
}

void JobTracker::enableSNMPManagement(void)
{
    vector<string> switchNodes = getSwitches();
    nextSNMPInterval = max((long)(1000 * ceil((double)getTaskTrackerCount() / CLUSTER_INCREMENT)), SNMP_INTERVAL_MIN);
    for(vector<string>::const_iterator it = switchNodes.cbegin(); it != switchNodes.cend(); it++) {
        vector<LinkId> links = getNodeOutLinks(*it);
        for(size_t i = 0; i < links.size(); ++i) {
            linkStats[links[i]] = getNetSim()->GetLinkStat(links[i]);
        }
        // add first SNMP event from all switches to the EventQueue
        long timeStamp = rand() % clusterStartupDuration;
        ns3::Simulator::Schedule(ns3::Seconds((double)timeStamp/1000.0), &JobTracker::sendSNMPMsg, this, *it);
    }
}

void JobTracker::collectLinkStat(vector<string> nodeNames)
{
    fprintf(netoptFile, "%lu\n", ns3::Simulator::Now().GetMilliSeconds());
    for(vector<string>::iterator it = nodeNames.begin(); it != nodeNames.end(); it++) {
        vector<LinkId> links = getNodeOutLinks(*it);
        unordered_map<LinkId, queue<LinkInfo>> linkInfoMap;
        queue<LinkInfo> linkInfoQueue;
        for(size_t i = 0; i < links.size(); ++i) {
            ns3::Ptr<LinkStat> stat = getNetSim()->GetLinkStat(links[i]);
            assert(stat->id() == links[i]);
            LinkInfo info;
            info.linkID = stat->id();
            info.bandwidth = stat->bandwidth_utilization(linkStats.at(info.linkID));
            info.queue = stat->queue_utilization();
            linkStats[info.linkID] = stat;
            fprintf(netoptFile, "LinkStat(%d) bandwidth=%02.2f%% queue=%02.2f%%\n", info.linkID, info.bandwidth*100, info.queue*100);

            if (nodeLinkStat.find(*it) != nodeLinkStat.end()) {
                linkInfoMap = nodeLinkStat[*it];
                if (linkInfoMap.find(links[i]) != linkInfoMap.end()) {
                    linkInfoQueue = linkInfoMap[links[i]];
                }
            }
            while(linkInfoQueue.size() >= LinkStatQueueLen) {
                linkInfoQueue.pop();
            }
            linkInfoQueue.push(info);
            linkInfoMap[links[i]] = linkInfoQueue;
            nodeLinkStat[*it] = linkInfoMap;
        }
    }

    // debug only
    /*
    cout<<"==All Nodes Link Stat== at "<<ns3::Simulator::Now().GetMilliSeconds()<<endl;
    for(unordered_map<string, unordered_map<LinkId, queue<LinkInfo>>>::iterator it = nodeLinkStat.begin(); it != nodeLinkStat.end(); ++it) {
        cout<<"NodeName: "<<it->first<<endl;
        for(unordered_map<LinkId, queue<LinkInfo>>::iterator itt = it->second.begin(); itt != it->second.end(); ++itt) {
            cout<<"\tQueueSize="<<itt->second.size()<<" ** Last Record ** linkID = "<<itt->second.back().linkID<<", bandwidth = "<<itt->second.back().bandwidth<<", queue = "<<itt->second.back().queue<<endl;
        }
    }
    cout<<"== Link Stat End ==\n\n";
    */
    fprintf(netoptFile, "\n");
    printf("%lu\n", ns3::Simulator::Now().GetMilliSeconds());
    ns3::Simulator::Schedule(ns3::Seconds(3.0), &JobTracker::collectLinkStat, this, nodeNames);
}

void JobTracker::enableNetOPT(void)
{
    vector<string> switchNodes = getSwitches();
    vector<string> slaveNodes = getClusterSlaveNodeName();
    vector<string> allNodes;
    allNodes.reserve(switchNodes.size() + slaveNodes.size());
    allNodes.insert(allNodes.end(), switchNodes.begin(), switchNodes.end());
    allNodes.insert(allNodes.end(), slaveNodes.begin(), slaveNodes.end());
    allNodes.push_back(getHostName());
    for(vector<string>::iterator it = allNodes.begin(); it != allNodes.end(); it++) {
        vector<LinkId> links = getNodeOutLinks(*it);
        for(size_t i = 0; i < links.size(); ++i) {
            linkStats[links[i]] = getNetSim()->GetLinkStat(links[i]);
        }
    }
    ns3::Simulator::Schedule(ns3::Seconds(3.0), &JobTracker::collectLinkStat, this, allNodes);
}

void initJobTracker(string hostName, int schedType, string debugDir, long MapCPUTime, long scaledDownRatio, long mapNum, long reduceNum)
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

    if (schedType == 1) {
        //jobTracker->enableSNMPManagement();
        jobTracker->enableNetOPT();
        netoptFile = fopen((debugDir + "linkstat.txt").c_str(), "w");
    }

    scaledMapCPUTime = MapCPUTime;
    scaledDownRatioForReduce = scaledDownRatio;
    customMapNum = mapNum;
    customReduceNum = reduceNum;
}

void killJobTracker(void)
{
    if(netoptFile)
        fclose(netoptFile);
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
