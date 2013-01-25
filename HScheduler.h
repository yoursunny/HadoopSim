#ifndef HSCHEDULER_H
#define HSCHEDULER_H

#include <list>
#include <string>
#include "Task.h"

class HScheduler {
public:
    HScheduler(std::string name, int id): schedName(name), schedID(id) { }
    std::string getSchedName() { return schedName; }
    int getSchedID() { return schedID; }
    virtual std::list<TaskAction> assignTasks(std::string trackerName, long numAvailMapSlots, long numAvailReduceSlots, long now) = 0;
private:
    std::string schedName;
    int schedID;
};

class DataLocalityScheduler: public HScheduler {
public:
    DataLocalityScheduler(std::string name, int id): HScheduler(name, id) { }
    std::list<TaskAction> assignTasks(std::string trackerName, long numAvailMapSlots, long numAvailReduceSlots, long now);
};

class FIFOScheduler: public HScheduler {
public:
    FIFOScheduler(std::string name, int id): HScheduler(name, id) { }
    std::list<TaskAction> assignTasks(std::string trackerName, long numAvailMapSlots, long numAvailReduceSlots, long now);
};

#endif // HSCHEDULER_H
