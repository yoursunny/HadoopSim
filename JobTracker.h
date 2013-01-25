#ifndef JOBTRACKER_H
#define JOBTRACKER_H

#include <list>
#include <map>
#include <queue>
#include <vector>
#include "Cluster.h"
#include "HeartBeat.h"
#include "HScheduler.h"
#include "Job.h"
#include "JobTaskStory.h"
#include "TaskTrackerStatus.h"

typedef struct LinkInfo {
    int linkID;
    float bandwidth;
    float queue;
}LinkInfo;

class JobTracker {
public:
    JobTracker(std::string hostName, HScheduler *sched);
    void updateBlockNodeMapping(std::string splitID, std::vector<std::string> dataNodes);
    void handleBlockPlacement(std::string splitID);
    void acceptNewJob(JobStory *jobStory, long now);
    void updateTaskTrackerStatus(HeartBeatReport report, long now);
    void updateTaskStatus(HeartBeatReport report, long now);
    TaskTrackerStatus getTaskTrackerStatus(std::string trackerName);
    std::vector<MapDataAction> issueMapDataAction(std::string taskTrackerName);      // new scheduler feature based on NetOPT
    HeartBeatResponse processHeartbeat(HeartBeatReport report, long now);
    std::map<std::string, Job> &getRunningJobs();
    const std::map<std::string, Job> &getCompletedJobs() const;
    const std::map<std::string, std::vector<std::string>> &getNode2Block() const;
    const std::map<std::string, std::vector<std::string>> &getBlock2Node() const;
    const std::string getHostName(void) const;
    const size_t getTaskTrackerCount(void) const;
    const long getMapSlotCapacity(void) const;
    void sendSNMPMsg(std::string switchName);
    void receiveSNMPMsg(ns3::Ptr<HadoopNetSim::MsgInfo> msg);
    void enableSNMPManagement(void);
    void collectLinkStat(std::vector<std::string> nodeNames);
    void enableNetOPT(void);
private:
    std::string hostName;
    HScheduler *sched;
    std::list<TaskAction> taskActions;
    std::map<std::string, std::vector<std::string>> node2Block;    // trackerName <---> splitIDs on this tracker
    std::map<std::string, std::vector<std::string>> block2Node;    // splitID <---> trackerNames holding this split
    std::map<std::string, long> nodeBlockCnt;    // trackerName <---> # of blocks on this node
    std::map<std::string, TaskTrackerStatus> allTaskTrackerStatus;  // trackerName <--> TaskTrackerStatus
    std::map<std::string, std::vector<MapDataAction>> allMapDataActions;  // trackerName <--> vector<MapDataAction> on this tracker
    std::map<std::string, Job> runningJobs;
    std::map<std::string, Job> completedJobs;

    // --- Network Information only used for new scheduler ---
    std::unordered_map<HadoopNetSim::LinkId,ns3::Ptr<HadoopNetSim::LinkStat>> linkStats;
    std::unordered_map<HadoopNetSim::MsgId, std::vector<LinkInfo>> snmpMsg;      // snmp msg buffer (no sure if these msges arrive at JobTracker)
    std::unordered_map<std::string, std::unordered_map<HadoopNetSim::LinkId, std::queue<LinkInfo>>> nodeLinkStat; // link stat queue for each node (including switch + tasktracker)
};

void initJobTracker(std::string hostName, int schedType, std::string debugDir, long MapCPUTime, long scaledDownRatio, long mapNum, long reduceNum);
void killJobTracker(void);
JobTracker *getJobTracker(void);
const std::string getJobTrackerName(void);

#endif // JOBTRACKER_H
