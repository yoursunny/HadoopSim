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
