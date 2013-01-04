#include "netsim/linkstat.h"
namespace HadoopNetSim {

LinkStat::LinkStat(ns3::Ptr<LinkStatReader> lsr, uint32_t total_bytes, uint32_t queue_length) {
  assert(lsr != NULL);
  this->lsr_ = lsr;
  this->timestamp_ = ns3::Simulator::Now().GetSeconds();
  this->total_bytes_ = total_bytes;
  this->queue_length_ = queue_length;
}

float LinkStat::bandwidth_utilization(const ns3::Ptr<LinkStat> previous) const {
  assert(this->id() == previous->id());
  return this->lsr_->CalcBandwidthUtilization(previous->total_bytes_, previous->timestamp_, this->total_bytes_, this->timestamp_);
}

float LinkStat::queue_utilization(void) const {
  return this->lsr_->CalcQueueUtilization(this->queue_length_);
}

LinkStatReader::LinkStatReader(LinkId id, ns3::Ptr<ns3::NetDevice> device) {
  assert(id != LinkId_invalid);
  this->id_ = id;

  assert(device != NULL);
  this->device_ = device;
  ns3::DataRateValue rate;
  if (!this->device_->GetAttributeFailSafe("DataRate", rate)) {
    this->device_->GetChannel()->GetAttribute("DataRate", rate);
  }
  this->datarate_ = rate.Get();
  
  ns3::PointerValue ptr_queue;
  this->device_->GetAttribute("TxQueue", ptr_queue);
  ns3::Ptr<ns3::Queue> queue = ptr_queue.Get<ns3::Queue>();
  assert(queue != NULL);
  ns3::TypeId queue_type = queue->GetInstanceTypeId();
  assert(queue_type == ns3::DropTailQueue::GetTypeId());
  this->queue_ = queue->GetObject<ns3::DropTailQueue>();
  ns3::DropTailQueue::QueueMode queue_mode = this->queue_->GetMode();
  switch (queue_mode) {
    case ns3::DropTailQueue::QUEUE_MODE_BYTES: {
      ns3::UintegerValue max_bytes; this->queue_->GetAttribute("MaxBytes", max_bytes);
      this->queue_capacity_ = max_bytes.Get();
    } break;
    case ns3::DropTailQueue::QUEUE_MODE_PACKETS: {
      ns3::UintegerValue max_pkts; this->queue_->GetAttribute("MaxPackets", max_pkts);
      this->queue_capacity_ = max_pkts.Get();
    } break;
    default: assert(false); break;
  }
}

ns3::TypeId LinkStatReader::GetTypeId(void) {
  static ns3::TypeId tid = ns3::TypeId("HadoopNetSim::LinkStatReader")
                           .SetParent<ns3::Object>();
  return tid;
}

uint64_t LinkStatReader::bandwidth() const {
  return this->datarate_.GetBitRate() >> 3;
}


ns3::Ptr<LinkStat> LinkStatReader::Read(void) {
  uint32_t total_bytes = this->queue_->GetTotalReceivedBytes();

  uint32_t queue_length = 0;
  ns3::DropTailQueue::QueueMode queue_mode = this->queue_->GetObject<ns3::DropTailQueue>()->GetMode();
  switch (queue_mode) {
    case ns3::DropTailQueue::QUEUE_MODE_BYTES: {
      queue_length = this->queue_->GetNBytes();
    } break;
    case ns3::DropTailQueue::QUEUE_MODE_PACKETS: {
      queue_length = this->queue_->GetNPackets();
    } break;
    default: assert(false); break;
  }
  
  return ns3::Create<LinkStat>(this, total_bytes, queue_length);
}

float LinkStatReader::CalcBandwidthUtilization(uint32_t total_bytes_prev, double timestamp_prev, uint32_t total_bytes_now, double timestamp_now) {
  uint32_t bytes = total_bytes_now - total_bytes_prev;
  double time = timestamp_now - timestamp_prev;
  assert(time > 0);
  return (float)(this->datarate_.CalculateTxTime(bytes) / time);
}

float LinkStatReader::CalcQueueUtilization(uint32_t queue_length) {
  return (float)queue_length / this->queue_capacity_;
}

};//namespace HadoopNetSim
