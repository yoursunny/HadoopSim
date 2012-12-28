/*
Lei Ye <leiy@cs.arizona.edu>
HadoopSim is a simulator for a Hadoop Runtime by replaying the collected traces.
*/
#include "HEvent.h"

HEvent::HEvent(EventListener *listener, int type)
{
    this->listener = listener;
    this->type = (EvtType)type;
}

EventListener *HEvent::getListener()
{
    return this->listener;
}

EvtType HEvent::getType()
{
    return this->type;
}
