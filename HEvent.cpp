/*
Lei Ye <leiy@cs.arizona.edu>
HadoopSim is a simulator for a Hadoop Runtime by replaying the collected traces.
*/
#include <iostream>
#include "HEvent.h"
using namespace std;

HEvent::HEvent(EventListener *listener, int type, long timestamp)
{
    this->listener = listener;
    this->type = (EvtType)type;
    this->timestamp = timestamp;
}

EventListener *HEvent::getListener()
{
    return this->listener;
}

EvtType HEvent::getType()
{
    return this->type;
}

long HEvent::getTimeStamp()
{
    return this->timestamp;
}

long HEvent::getVersion()
{
    return this->version;
}

void HEvent::setVersion(long count)
{
    this->version = count;
}

