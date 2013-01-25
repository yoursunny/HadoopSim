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
    virtual void handleNewEvent(EvtType type) { }
};

class HEvent {  // Hadoop Event Class
public:
    HEvent():type(EVT_NULL) {}
    HEvent(EventListener *listener, int type);
    EventListener *getListener();
    EvtType getType();

private:
    EventListener *listener;
    EvtType type;       // Event type
};

#endif // HEVENT_H
