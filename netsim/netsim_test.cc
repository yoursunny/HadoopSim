#include "netsim/netsim.h"
#include <unordered_map>
#include "gtest/gtest.h"
namespace HadoopNetSim {

/*
TOPOLOGY
manager0 \              / slave0
           sw1 ---- sw2 - manager1
  slave1 /              \ slave2

EVENTS after ready
0,3,6 seconds
  slaves send NameRequest (1KB) to managers
  reply NameResponse (2KB)
2 seconds
  slaves send 3x DataRequest (256B) to all other slaves
  reply DataResponse (64MB)
3.1 seconds
  show link stat
8 seconds
  stop
*/

class NetSimTestRunner {
  public:
    NetSimTestRunner(NetSim* netsim) {
      this->netsim_ = netsim;
      this->netsim_->set_ready_cb(ns3::MakeCallback(&NetSimTestRunner::Ready, this));
      this->userobj_ = this;
    }
    void Verify() {
      std::unordered_map<MsgType,ns3::Ptr<ns3::MinMaxAvgTotalCalculator<double>>,std::hash<int>> calcs;
      calcs[kMTNameRequest] = ns3::Create<ns3::MinMaxAvgTotalCalculator<double>>();
      calcs[kMTNameResponse] = ns3::Create<ns3::MinMaxAvgTotalCalculator<double>>();
      calcs[kMTDataRequest] = ns3::Create<ns3::MinMaxAvgTotalCalculator<double>>();
      calcs[kMTDataResponse] = ns3::Create<ns3::MinMaxAvgTotalCalculator<double>>();
      for (std::unordered_map<MsgId,ns3::Ptr<MsgInfo>>::const_iterator it = this->received_.cbegin(); it != this->received_.cend(); ++it) {
        ns3::Ptr<MsgInfo> msg = it->second;
        calcs[msg->type()]->Update((msg->finish() - msg->start()).GetSeconds());
      }
      assert(calcs[kMTNameRequest]->getCount() == 18);
      assert(calcs[kMTNameResponse]->getCount() == 18);
      assert(calcs[kMTDataRequest]->getCount() == 18);
      assert(calcs[kMTDataResponse]->getCount() == 18);
      
      printf("NameRequest %f,%f,%f\n", calcs[kMTNameRequest]->getMin(), calcs[kMTNameRequest]->getMean(), calcs[kMTNameRequest]->getMax());
      printf("NameResponse %f,%f,%f\n", calcs[kMTNameResponse]->getMin(), calcs[kMTNameResponse]->getMean(), calcs[kMTNameResponse]->getMax());
      printf("DataRequest %f,%f,%f\n", calcs[kMTDataRequest]->getMin(), calcs[kMTDataRequest]->getMean(), calcs[kMTDataRequest]->getMax());
      printf("DataResponse %f,%f,%f\n", calcs[kMTDataResponse]->getMin(), calcs[kMTDataResponse]->getMean(), calcs[kMTDataResponse]->getMax());
    }
    
  private:
    void* userobj_;
    NetSim* netsim_;
    std::unordered_map<MsgId,MsgType> sent_;
    std::unordered_map<MsgId,ns3::Ptr<MsgInfo>> received_;
    int remaining_namerequestall_;
    
    void Ready(NetSim*) {
      this->remaining_namerequestall_ = 2;
      ns3::Simulator::Schedule(ns3::Seconds(0.0), &NetSimTestRunner::NameRequestAll, this);
      for (int i = 0; i < 3; ++i) {
        ns3::Simulator::Schedule(ns3::Seconds(2.0), &NetSimTestRunner::DataRequestAll, this);
      }
      ns3::Simulator::Schedule(ns3::Seconds(3.1), &NetSimTestRunner::ShowLinkStat, this);
      ns3::Simulator::Schedule(ns3::Seconds(8.0), &ns3::Simulator::Stop);
    }
    
    void NameRequestAll() {
      MsgId id;
      id = this->netsim_->NameRequest("slave0", "manager0", 1<<10, ns3::MakeCallback(&NetSimTestRunner::NameResponse, this), this->userobj_);
      assert(id != MsgId_invalid);
      this->sent_[id] = kMTNameRequest;
      id = this->netsim_->NameRequest("slave0", "manager1", 1<<10, ns3::MakeCallback(&NetSimTestRunner::NameResponse, this), this->userobj_);
      assert(id != MsgId_invalid);
      this->sent_[id] = kMTNameRequest;
      id = this->netsim_->NameRequest("slave1", "manager0", 1<<10, ns3::MakeCallback(&NetSimTestRunner::NameResponse, this), this->userobj_);
      assert(id != MsgId_invalid);
      this->sent_[id] = kMTNameRequest;
      id = this->netsim_->NameRequest("slave1", "manager1", 1<<10, ns3::MakeCallback(&NetSimTestRunner::NameResponse, this), this->userobj_);
      assert(id != MsgId_invalid);
      this->sent_[id] = kMTNameRequest;
      id = this->netsim_->NameRequest("slave2", "manager0", 1<<10, ns3::MakeCallback(&NetSimTestRunner::NameResponse, this), this->userobj_);
      assert(id != MsgId_invalid);
      this->sent_[id] = kMTNameRequest;
      id = this->netsim_->NameRequest("slave2", "manager1", 1<<10, ns3::MakeCallback(&NetSimTestRunner::NameResponse, this), this->userobj_);
      assert(id != MsgId_invalid);
      this->sent_[id] = kMTNameRequest;
      
      if (--this->remaining_namerequestall_ >= 0) {
        //schedule another batch 3 seconds later
        ns3::Simulator::Schedule(ns3::Seconds(3.0), &NetSimTestRunner::NameRequestAll, this);
      }
    }
    void NameResponse(ns3::Ptr<MsgInfo> request_msg) {
      assert(this->sent_[request_msg->id()] == kMTNameRequest);
      assert(this->received_.count(request_msg->id()) == 0);
      assert(request_msg->userobj() == this->userobj_);
      this->received_[request_msg->id()] = request_msg;
      MsgId id = this->netsim_->NameResponse(request_msg->dst(), request_msg->src(), 1<<11, ns3::MakeCallback(&NetSimTestRunner::NameFinish, this), this->userobj_);
      assert(id != MsgId_invalid);
      this->sent_[id] = kMTNameResponse;
    }
    void NameFinish(ns3::Ptr<MsgInfo> response_msg) {
      assert(this->sent_[response_msg->id()] == kMTNameResponse);
      assert(response_msg->userobj() == this->userobj_);
      assert(this->received_.count(response_msg->id()) == 0);
      this->received_[response_msg->id()] = response_msg;
    }
    void DataRequestAll() {
      MsgId id;
      id = this->netsim_->DataRequest("slave0", "slave1", 1<<8, ns3::MakeCallback(&NetSimTestRunner::DataResponse, this), this->userobj_);
      assert(id != MsgId_invalid);
      this->sent_[id] = kMTDataRequest;
      id = this->netsim_->DataRequest("slave0", "slave2", 1<<8, ns3::MakeCallback(&NetSimTestRunner::DataResponse, this), this->userobj_);
      assert(id != MsgId_invalid);
      this->sent_[id] = kMTDataRequest;
      id = this->netsim_->DataRequest("slave1", "slave2", 1<<8, ns3::MakeCallback(&NetSimTestRunner::DataResponse, this), this->userobj_);
      assert(id != MsgId_invalid);
      this->sent_[id] = kMTDataRequest;
      id = this->netsim_->DataRequest("slave1", "slave0", 1<<8, ns3::MakeCallback(&NetSimTestRunner::DataResponse, this), this->userobj_);
      assert(id != MsgId_invalid);
      this->sent_[id] = kMTDataRequest;
      id = this->netsim_->DataRequest("slave2", "slave0", 1<<8, ns3::MakeCallback(&NetSimTestRunner::DataResponse, this), this->userobj_);
      assert(id != MsgId_invalid);
      this->sent_[id] = kMTDataRequest;
      id = this->netsim_->DataRequest("slave2", "slave1", 1<<8, ns3::MakeCallback(&NetSimTestRunner::DataResponse, this), this->userobj_);
      assert(id != MsgId_invalid);
      this->sent_[id] = kMTDataRequest;
    }
    void DataResponse(ns3::Ptr<MsgInfo> request_msg) {
      assert(this->sent_[request_msg->id()] == kMTDataRequest);
      assert(request_msg->userobj() == this->userobj_);
      assert(this->received_.count(request_msg->id()) == 0);
      this->received_[request_msg->id()] = request_msg;
      MsgId id = this->netsim_->DataResponse(request_msg->id(), request_msg->dst(), request_msg->src(), 1<<26, ns3::MakeCallback(&NetSimTestRunner::DataFinish, this), this->userobj_);
      assert(id != MsgId_invalid);
      this->sent_[id] = kMTDataResponse;
    }
    void DataFinish(ns3::Ptr<MsgInfo> response_msg) {
      assert(this->sent_[response_msg->id()] == kMTDataResponse);
      assert(response_msg->userobj() == this->userobj_);
      assert(this->received_.count(response_msg->id()) == 0);
      this->received_[response_msg->id()] = response_msg;
    }
    void ShowLinkStat() {
      for (LinkId id = 1; id <= 6; ++id) {
        ns3::Ptr<LinkStat> stat = this->netsim_->GetLinkStat(id);
        printf("LinkStat(%d) bandwidth=%02.2f%% queue=%02.2f%%\n", stat->id(), stat->bandwidth_utilization()*100, stat->queue_utilization()*100);
        LinkId rid = -id;
        stat = this->netsim_->GetLinkStat(rid);
        printf("LinkStat(%d) bandwidth=%02.2f%% queue=%02.2f%%\n", stat->id(), stat->bandwidth_utilization()*100, stat->queue_utilization()*100);
      }
    }
    
    DISALLOW_COPY_AND_ASSIGN(NetSimTestRunner);
};

TEST(NetSimTest, NetSim) {
  Topology topology;
  char topo_json[] = "{\"version\":1,\"nodes\":{\"sw1\":{\"type\":\"switch\",\"devices\":[\"eth0\",\"eth1\",\"eth2\"]},\"sw2\":{\"type\":\"switch\",\"devices\":[\"eth0\",\"eth1\",\"eth2\",\"eth3\"]},\"manager0\":{\"type\":\"host\",\"ip\":\"10.0.0.1\",\"devices\":[\"eth0\"]},\"manager1\":{\"type\":\"host\",\"ip\":\"10.0.0.2\",\"devices\":[\"eth0\"]},\"slave0\":{\"type\":\"host\",\"ip\":\"10.0.1.1\",\"devices\":[\"eth0\"]},\"slave1\":{\"type\":\"host\",\"ip\":\"10.0.1.2\",\"devices\":[\"eth0\"]},\"slave2\":{\"type\":\"host\",\"ip\":\"10.0.1.3\",\"devices\":[\"eth0\"]}},\"links\":{\"1\":{\"node1\":\"sw1\",\"port1\":\"eth1\",\"node2\":\"manager0\",\"port2\":\"eth0\",\"type\":\"eth1G\"},\"2\":{\"node1\":\"sw2\",\"port1\":\"eth1\",\"node2\":\"manager1\",\"port2\":\"eth0\",\"type\":\"eth1G\"},\"3\":{\"node1\":\"sw2\",\"port1\":\"eth0\",\"node2\":\"slave0\",\"port2\":\"eth0\",\"type\":\"eth1G\"},\"4\":{\"node1\":\"sw1\",\"port1\":\"eth1\",\"node2\":\"slave1\",\"port2\":\"eth0\",\"type\":\"eth1G\"},\"5\":{\"node1\":\"sw2\",\"port1\":\"eth2\",\"node2\":\"slave2\",\"port2\":\"eth0\",\"type\":\"eth1G\"},\"6\":{\"node1\":\"sw1\",\"port1\":\"eth2\",\"node2\":\"sw2\",\"port2\":\"eth3\",\"type\":\"eth10G\"}}}";
  topology.LoadString(topo_json);
  std::unordered_set<HostName> managers; managers.insert("manager0"); managers.insert("manager1");

  EXPECT_EXIT({
    NetSim netsim;
    netsim.BuildTopology(topology);
    //if (netsim.GetHostIP("manager1") != ns3::Ipv4Address("10.0.0.2")) ::exit(1);
    netsim.InstallApps(managers);
    
    NetSimTestRunner runner(&netsim);
    ns3::Simulator::Run();
    ns3::Simulator::Destroy();
    runner.Verify();
    ::exit(0);
  }, ::testing::ExitedWithCode(0), "");
}

};//namespace HadoopNetSim
