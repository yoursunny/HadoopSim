/*
Lei Ye <leiy@cs.arizona.edu>
HadoopSim is a simulator for a Hadoop Runtime by replaying the collected traces.
*/
#ifndef HEVENT_H
#define HEVENT_H

typedef enum EvtType {
    EVT_NULL,
    EVT_HBReport,
    EVT_HBResponse,
    EVT_JobSubmit,
    EVT_JobDone,
    EVT_MapTaskDone,
    EVT_LoadProbe
}EvtType;

class EventListener {
public:
    virtual void handleNewEvent(long timestamp, EvtType type) { }
};

class HEvent {  // Hadoop Event Class
public:
    HEvent():type(EVT_NULL), timestamp(0), version(0) {}
    HEvent(EventListener *listener, int type, long timestamp);
    EventListener *getListener();
    EvtType getType();
    long getTimeStamp();
    long getVersion();
    void setVersion(long count);
private:
    EventListener *listener;
    EvtType type;       // Event type
    long timestamp;     // The timestamp of event (milliseconds)
    long version;       // Version number orders multiple events that occur at the same time.
};

#endif // HEVENT_H
