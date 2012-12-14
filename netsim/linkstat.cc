#include "netsim/linkstat.h"
namespace HadoopNetSim {

LinkStat::LinkStat(LinkId id, ns3::Ptr<ns3::NetDevice> device) {
  assert(id != 0);
  assert(device != NULL);
  this->id_ = id;
  
  ns3::Ptr<BandwidthUtilizationCollector> buc = device->GetObject<BandwidthUtilizationCollector>();
  assert(buc != NULL);
  this->bandwidth_utilization_ = buc->Read();
  
  this->queue_utilization_ = this->ReadQueueUtilization(device);
}

float LinkStat::ReadQueueUtilization(ns3::Ptr<ns3::NetDevice> device) {
  ns3::PointerValue ptr_queue;
  device->GetAttribute("TxQueue", ptr_queue);
  ns3::Ptr<ns3::Queue> queue = ptr_queue.Get<ns3::Queue>();
  ns3::TypeId queue_type = queue->GetInstanceTypeId();
  if (queue_type == ns3::DropTailQueue::GetTypeId()) {
    ns3::DropTailQueue::QueueMode queue_mode = queue->GetObject<ns3::DropTailQueue>()->GetMode();
    switch (queue_mode) {
      case ns3::DropTailQueue::QUEUE_MODE_BYTES: {
        ns3::UintegerValue max_bytes; queue->GetAttribute("MaxBytes", max_bytes);
        return (float)queue->GetNBytes() / max_bytes.Get();
      }
      case ns3::DropTailQueue::QUEUE_MODE_PACKETS: {
        ns3::UintegerValue max_pkts; queue->GetAttribute("MaxPackets", max_pkts);
        return (float)queue->GetNPackets() / max_pkts.Get();
      }
      default: assert(false); break;
    }
  } else {
    assert(false);
  }
  return NAN;
}

BandwidthUtilizationCollector::BandwidthUtilizationCollector(void) {
  this->last_total_bytes_ = UINT32_MAX;
  this->scheduled_ = false;
}

ns3::TypeId BandwidthUtilizationCollector::GetTypeId(void) {
  static ns3::TypeId tid = ns3::TypeId("HadoopNetSim::BandwidthUtilizationCollector")
                           .SetParent<ns3::Object>()
                           .AddConstructor<BandwidthUtilizationCollector>();
  return tid;
}

void BandwidthUtilizationCollector::set_device(ns3::Ptr<ns3::NetDevice> value) {
  assert(!this->scheduled_);
  this->device_ = value;
  ns3::PointerValue ptr_queue;
  this->device_->GetAttribute("TxQueue", ptr_queue);
  this->queue_ = ptr_queue.Get<ns3::Queue>();
}

void BandwidthUtilizationCollector::Schedule(void) {
  assert(this->device_ != NULL);
  this->scheduled_ = true;
  this->ScheduleInternal();
}

float BandwidthUtilizationCollector::Read(void) {
  if (this->last_total_bytes_ == UINT32_MAX) return NAN;
  ns3::DataRateValue rate;
  if (!this->device_->GetAttributeFailSafe("DataRate", rate)) {
    this->device_->GetChannel()->GetAttribute("DataRate", rate);
  }
  return rate.Get().CalculateTxTime(this->last_total_bytes_);
}

void BandwidthUtilizationCollector::ScheduleInternal(void) {
  this->queue_->ResetStatistics();
  ns3::Simulator::Schedule(ns3::Seconds(1.0), &BandwidthUtilizationCollector::Collect, this);
}

void BandwidthUtilizationCollector::Collect(void) {
  this->last_total_bytes_ = this->queue_->GetTotalReceivedBytes();
  this->ScheduleInternal();
}


};//namespace HadoopNetSim
