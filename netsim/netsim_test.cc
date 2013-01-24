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
0.0,3.0,6.0 seconds
  slaves send NameRequest (1KB) to managers
  reply NameResponse (2KB)
1.8 seconds
  snapshot link stat
2.0 seconds
  slaves send 3x DataRequest (256B) to all other slaves
  reply DataResponse (64MB)
3.1 seconds
  snapshot and show link stat: queue util at 3.1, and bw util between 1.8 and 3.1
4.1,4.2,4.3,4.4,4.5 seconds
  sw1 send Snmp (2500B) to manager1
4.8 seconds
  snapshot link stat
4.8,4.9 seconds
  ImportRequest (64MB) on pipeline manager0-slave1-slave0-slave2
  reply ImportResponse (1KB)
5.3 seconds
  snapshot and show link stat: queue util at 5.3, and bw util between 4.8 and 5.3
12.0 seconds
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
      calcs[kMTSnmp] = ns3::Create<ns3::MinMaxAvgTotalCalculator<double>>();
      calcs[kMTImportRequest] = ns3::Create<ns3::MinMaxAvgTotalCalculator<double>>();
      calcs[kMTImportResponse] = ns3::Create<ns3::MinMaxAvgTotalCalculator<double>>();
      for (std::unordered_map<MsgId,ns3::Ptr<MsgInfo>>::const_iterator it = this->received_.cbegin(); it != this->received_.cend(); ++it) {
        ns3::Ptr<MsgInfo> msg = it->second;
        calcs[msg->type()]->Update((msg->finish() - msg->start()).GetSeconds());
      }
      assert(calcs[kMTNameRequest]->getCount() == 18);
      assert(calcs[kMTNameResponse]->getCount() == 18);
      assert(calcs[kMTDataRequest]->getCount() == 18);
      assert(calcs[kMTDataResponse]->getCount() == 18);
      assert(calcs[kMTSnmp]->getCount() >= 4);//sent 5 messages, expect at least 4 to arrive
      assert(calcs[kMTImportRequest]->getCount() == 2);
      assert(calcs[kMTImportResponse]->getCount() == 2);
      
      printf("NameRequest %f,%f,%f\n", calcs[kMTNameRequest]->getMin(), calcs[kMTNameRequest]->getMean(), calcs[kMTNameRequest]->getMax());
      printf("NameResponse %f,%f,%f\n", calcs[kMTNameResponse]->getMin(), calcs[kMTNameResponse]->getMean(), calcs[kMTNameResponse]->getMax());
      printf("DataRequest %f,%f,%f\n", calcs[kMTDataRequest]->getMin(), calcs[kMTDataRequest]->getMean(), calcs[kMTDataRequest]->getMax());
      printf("DataResponse %f,%f,%f\n", calcs[kMTDataResponse]->getMin(), calcs[kMTDataResponse]->getMean(), calcs[kMTDataResponse]->getMax());
      printf("Snmp(%"PRId64") %f,%f,%f\n", calcs[kMTSnmp]->getCount(), calcs[kMTSnmp]->getMin(), calcs[kMTSnmp]->getMean(), calcs[kMTSnmp]->getMax());
      printf("ImportRequest %f,%f,%f\n", calcs[kMTImportRequest]->getMin(), calcs[kMTImportRequest]->getMean(), calcs[kMTImportRequest]->getMax());
      printf("ImportResponse %f,%f,%f\n", calcs[kMTImportResponse]->getMin(), calcs[kMTImportResponse]->getMean(), calcs[kMTImportResponse]->getMax());
    }

  private:
    void* userobj_;
    NetSim* netsim_;
    std::unordered_map<MsgId,MsgType> sent_;
    std::unordered_map<MsgId,ns3::Ptr<MsgInfo>> received_;
    int remaining_namerequestall_;
    std::unordered_map<LinkId,ns3::Ptr<LinkStat>> linkstat_;//at 1.8

    void Ready(NetSim*) {
      this->remaining_namerequestall_ = 2;
      ns3::Simulator::Schedule(ns3::Seconds(0.0), &NetSimTestRunner::NameRequestAll, this);
      for (int i = 0; i < 3; ++i) {
        ns3::Simulator::Schedule(ns3::Seconds(2.0), &NetSimTestRunner::DataRequestAll, this);
      }
      ns3::Simulator::Schedule(ns3::Seconds(1.8), &NetSimTestRunner::SaveLinkStat, this);
      ns3::Simulator::Schedule(ns3::Seconds(3.1), &NetSimTestRunner::ShowLinkStat, this);
      for (double t = 4.1; t < 4.51; t += 0.1) {
        ns3::Simulator::Schedule(ns3::Seconds(t), &NetSimTestRunner::SnmpSend, this);
      }
      ns3::Simulator::Schedule(ns3::Seconds(4.8), &NetSimTestRunner::ImportRequest, this);
      ns3::Simulator::Schedule(ns3::Seconds(4.9), &NetSimTestRunner::ImportRequest, this);
      ns3::Simulator::Schedule(ns3::Seconds(4.8), &NetSimTestRunner::SaveLinkStat, this);
      ns3::Simulator::Schedule(ns3::Seconds(5.3), &NetSimTestRunner::ShowLinkStat, this);
      ns3::Simulator::Schedule(ns3::Seconds(12.0), &ns3::Simulator::Stop);
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
    void SnmpSend() {
      MsgId id;
      id = this->netsim_->Snmp("sw1", "manager1", 2500, ns3::MakeCallback(&NetSimTestRunner::SnmpArrive, this), this->userobj_);
      assert(id != MsgId_invalid);
      this->sent_[id] = kMTSnmp;
    }
    void SnmpArrive(ns3::Ptr<MsgInfo> msg) {
      assert(this->sent_[msg->id()] == kMTSnmp);
      assert(msg->userobj() == this->userobj_);
      //it's UDP, cannot assume not duplicate
      this->received_[msg->id()] = msg;
    }
    void ImportRequest() {
      std::vector<HostName> pipeline; pipeline.push_back("manager0"); pipeline.push_back("slave1"); pipeline.push_back("slave0"); pipeline.push_back("slave2");
      MsgId id = this->netsim_->ImportRequest(pipeline, 1<<26, ns3::MakeCallback(&NetSimTestRunner::ImportResponse, this), this->userobj_);
      assert(id != MsgId_invalid);
      this->sent_[id] = kMTImportRequest;
    }
    void ImportResponse(ns3::Ptr<MsgInfo> request_msg) {
      assert(this->sent_[request_msg->id()] == kMTImportRequest);
      assert(request_msg->userobj() == this->userobj_);
      assert(this->received_.count(request_msg->id()) == 0);
      this->received_[request_msg->id()] = request_msg;
      std::vector<HostName> pipeline; pipeline.push_back("slave2"); pipeline.push_back("slave0"); pipeline.push_back("slave1"); pipeline.push_back("manager0");
      MsgId id = this->netsim_->ImportResponse(request_msg->id(), pipeline, 1<<10, ns3::MakeCallback(&NetSimTestRunner::ImportFinish, this), this->userobj_);
      assert(id != MsgId_invalid);
      this->sent_[id] = kMTImportResponse;
    }
    void ImportFinish(ns3::Ptr<MsgInfo> response_msg) {
      assert(this->sent_[response_msg->id()] == kMTImportResponse);
      assert(response_msg->userobj() == this->userobj_);
      assert(this->received_.count(response_msg->id()) == 0);
      this->received_[response_msg->id()] = response_msg;
    }
    
    void SaveLinkStat() {
      for (LinkId id = -6; id <= 6; ++id) {
        if (id == LinkId_invalid) continue;
        this->linkstat_[id] = this->netsim_->GetLinkStat(id);
      }
    }
    void ShowLinkStat() {
      for (LinkId id = -6; id <= 6; ++id) {
        if (id == LinkId_invalid) continue;
        ns3::Ptr<LinkStat> previous = this->linkstat_.at(id);
        ns3::Ptr<LinkStat> stat = this->netsim_->GetLinkStat(id);
        printf("LinkStat(%d) bandwidth=%02.2f%% queue=%02.2f%%\n", stat->id(), stat->bandwidth_utilization(previous)*100, stat->queue_utilization()*100);
      }
    }
    
    DISALLOW_COPY_AND_ASSIGN(NetSimTestRunner);
};

TEST(NetSimTest, NetSim) {
  Topology topology;
  char topo_json[] = "{\"version\":1,\"type\":\"generic\",\"nodes\":{\"sw1\":{\"type\":\"switch\",\"devices\":[\"eth0\",\"eth1\",\"eth2\"]},\"sw2\":{\"type\":\"switch\",\"devices\":[\"eth0\",\"eth1\",\"eth2\",\"eth3\"]},\"manager0\":{\"type\":\"host\",\"ip\":\"10.0.0.1\",\"devices\":[\"eth0\"]},\"manager1\":{\"type\":\"host\",\"ip\":\"10.0.0.2\",\"devices\":[\"eth0\"]},\"slave0\":{\"type\":\"host\",\"ip\":\"10.0.1.1\",\"devices\":[\"eth0\"]},\"slave1\":{\"type\":\"host\",\"ip\":\"10.0.1.2\",\"devices\":[\"eth0\"]},\"slave2\":{\"type\":\"host\",\"ip\":\"10.0.1.3\",\"devices\":[\"eth0\"]}},\"links\":{\"1\":{\"node1\":\"sw1\",\"port1\":\"eth1\",\"node2\":\"manager0\",\"port2\":\"eth0\",\"type\":\"eth1G\"},\"2\":{\"node1\":\"sw2\",\"port1\":\"eth1\",\"node2\":\"manager1\",\"port2\":\"eth0\",\"type\":\"eth1G\"},\"3\":{\"node1\":\"sw2\",\"port1\":\"eth0\",\"node2\":\"slave0\",\"port2\":\"eth0\",\"type\":\"eth1G\"},\"4\":{\"node1\":\"sw1\",\"port1\":\"eth1\",\"node2\":\"slave1\",\"port2\":\"eth0\",\"type\":\"eth1G\"},\"5\":{\"node1\":\"sw2\",\"port1\":\"eth2\",\"node2\":\"slave2\",\"port2\":\"eth0\",\"type\":\"eth1G\"},\"6\":{\"node1\":\"sw1\",\"port1\":\"eth2\",\"node2\":\"sw2\",\"port2\":\"eth3\",\"type\":\"eth10G\"}}}";
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
