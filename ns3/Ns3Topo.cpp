/*
Lei Ye <leiy@cs.arizona.edu>
HadoopSim is a simulator for a Hadoop Runtime by replaying the collected traces.
*/
#include <assert.h>
#include <iostream>
#include <map>
#include <sstream>
#include <string>
#include <vector>
#include "Ns3.h"
#include "Ns3DataNode.h"
#include "Ns3NameNode.h"
using namespace ns3;
using namespace std;

const string NetworkAddress = "10.1.0.0";
const string NetworkMask = "255.255.0.0";
const uint16_t NameNodeServerPort = 2009;   // well-known NameNodeServer port number.
const uint16_t DataNodeServerPort = 2013;   // well-known DataNodeServer port number.
static NodeContainer nameNodeClientNodes;
static ApplicationContainer nameNodeClientApps;
static map<string, uint32_t> IP2NameNodeClientAppIndex;

// Send a heartbeat to NameNodeServer from NameNodeClient on a DataNode whoes IP
// Address is src.
void transferHeartBeat(string src, size_t bytes, Time now)
{
    // find NameNodeClient using src IP Addr
    map<string, uint32_t>::iterator it;
    it = IP2NameNodeClientAppIndex.find(src);
    assert(it != IP2NameNodeClientAppIndex.end());
    Ptr<Application> app = nameNodeClientApps.Get(it->second);

    // SetDataSize
    app->GetObject<NameNodeClient>()->SetDataSize(bytes);

    // ScheduleTransmit
    app->GetObject<NameNodeClient>()->ScheduleTransmit(Simulator::Now());
}

void fetchRawData(string dest, string src, size_t bytes, uint32_t dataType, uint32_t dataRequestID, Time now)
{
    // create DataNodeClient using src IP Addr
    NS_LOG_COMPONENT_DEFINE("DataNodeClient");
    LogComponentEnable("DataNodeClient", LOG_LEVEL_ALL);
    NS_LOG_INFO("Create DataNodeClient Application.");
    DataNodeClientHelper dataNodeClient(Ipv4Address(dest.c_str()), DataNodeServerPort);
    dataNodeClient.SetAttribute("MaxPackets", UintegerValue(1));
    dataNodeClient.SetAttribute("Interval", TimeValue(Seconds(1.0)));
    dataNodeClient.SetAttribute("PacketSize", UintegerValue(183));

    // find the node to install the DataNodeClient
    map<string, uint32_t>::iterator it;
    it = IP2NameNodeClientAppIndex.find(src);
    assert(it != IP2NameNodeClientAppIndex.end());
    Ptr<Node> node = nameNodeClientNodes.Get(it->second);
    ApplicationContainer dataNodeClientApps = dataNodeClient.Install(node);
//    dataNodeClientApps.Start(now);

    // SetDataSize
    assert(dataNodeClientApps.GetN() == 1);
    DataRequest request;
    request.dataType = dataType;
    request.dataRequestID = dataRequestID;
    request.requestBytes = 300;//bytes;
    dataNodeClientApps.Get(0)->GetObject<DataNodeClient>()->SetDataSize(sizeof(DataRequest));
    dataNodeClientApps.Get(0)->GetObject<DataNodeClient>()->SetFill((uint8_t *)&request, sizeof(DataRequest), sizeof(DataRequest));
    dataNodeClientApps.Start(now);
    // ScheduleTransmit
//    dataNodeClientApps.Get(0)->GetObject<DataNodeClient>()->ScheduleTransmit(Seconds(0.));
}

void fetchMapData(string dest, string src, size_t bytes, uint32_t dataType, uint32_t dataRequestID, Time now)
{
    return fetchRawData(dest, src, bytes, dataType, dataRequestID, now);
}

void buildBusTopo(vector<MachineNode> &nodeSet)
{
    NS_LOG_COMPONENT_DEFINE("BusTopologyNetwork");
    LogComponentEnable("BusTopologyNetwork", LOG_LEVEL_ALL);
    LogComponentEnable("NameNodeApplication", LOG_LEVEL_INFO);
    LogComponentEnable("DataNodeApplication", LOG_LEVEL_INFO);
    NS_LOG_INFO("Bus Network Simulation");

    NodeContainer csmaNodes;
    csmaNodes.Create(nodeSet.size() + 1); // # of nodes ( 1 NameNode + N DataNodes)

    CsmaHelper csma;
    csma.SetChannelAttribute("DataRate", StringValue("1Gbps"));
    csma.SetChannelAttribute("Delay", TimeValue(NanoSeconds(6560)));

    NetDeviceContainer csmaDevices;
    csmaDevices = csma.Install(csmaNodes);

    InternetStackHelper stack;
    stack.Install(csmaNodes);

    Ipv4AddressHelper address;
    address.SetBase(Ipv4Address(NetworkAddress.c_str()), Ipv4Mask(NetworkMask.c_str()));
    Ipv4InterfaceContainer csmaInterfaces;
    csmaInterfaces = address.Assign(csmaDevices);

    uint32_t size = csmaNodes.GetN();
    assert(size = nodeSet.size() + 1);
    for(uint32_t i = 0; i < size; i++) {
        Ptr<Node> node = csmaNodes.Get(i);
        Ptr<Ipv4> ipv4 = node->GetObject<Ipv4>();
        Ipv4InterfaceAddress iaddr = ipv4->GetAddress(1, 0);
        Ipv4Address addri = iaddr.GetLocal();
        stringstream ss;
        if (i == 0) {   // Server:  NameNode & JobTracker
            // Install NameNodeServer application
            cout<<"NameNode IPv4 Address = "<<addri<<endl;
            NS_LOG_INFO("Create NameNodeServer Application.");
            NameNodeServerHelper nameNodeServer(NameNodeServerPort);
            ApplicationContainer nameNodeServerApps = nameNodeServer.Install(node);
            nameNodeServerApps.Start(Seconds(0.));
            //nameNodeServerApps.Stop(Seconds(10.0));
        } else {    // Client:  DataNode & TaskTracker
            cout<<"NameNode IPv4 Address = "<<addri<<endl;
            ss<<addri;
            nodeSet[i - 1].setIpAddr(ss.str());
            nameNodeClientNodes.Add(node);
            // update the ip to NameNodeClientAppIndex
            IP2NameNodeClientAppIndex.insert(pair<string, uint32_t>(ss.str(), i - 1));
        }
    }

    // Install NameNodeClient applications
    NS_LOG_INFO("Create NameNodeClient Applications.");
    NameNodeClientHelper nameNodeClient(csmaInterfaces.GetAddress(0), NameNodeServerPort);
    nameNodeClient.SetAttribute("MaxPackets", UintegerValue(1));
    nameNodeClient.SetAttribute("Interval", TimeValue(Seconds(1.0)));
    nameNodeClient.SetAttribute("PacketSize", UintegerValue(183));
    nameNodeClientApps = nameNodeClient.Install(nameNodeClientNodes);
    nameNodeClientApps.Start(Seconds(0.));
    //nameNodeClientApps.Stop (Seconds (10.0));

    // Install DataNodeServer applications
    NS_LOG_INFO("Create DataNodeServer Applications.");
    DataNodeServerHelper dataNodeServer(DataNodeServerPort);
    ApplicationContainer dataNodeServerApps = dataNodeServer.Install(nameNodeClientNodes);
    dataNodeServerApps.Start(Seconds(0.));
    //dataNodeServerApps.Stop (Seconds (10.0));

    Ipv4GlobalRoutingHelper::PopulateRoutingTables();
}

void buildStarTopo(vector<MachineNode> &nodeSet)
{
}

void buildDumbBellTopo(vector<MachineNode> &nodeSet)
{
}

void buildTreeTopo(vector<MachineNode> &nodeSet)
{
}

void buildFatTreeTopo(vector<MachineNode> &nodeSet)
{
    for(size_t j = 0; j < nodeSet.size(); j++) {
        nodeSet[j].setIpAddr("ns3 ip");
    }
}

void setTopology(int topoType, vector<MachineNode> &nodeSet)
{
    switch(topoType) {
        case BUS:
            buildBusTopo(nodeSet);
            break;
        case STAR:
            buildStarTopo(nodeSet);
            break;
        case DUMBBELL:
            buildDumbBellTopo(nodeSet);
            break;
        case TREE:
            buildTreeTopo(nodeSet);
            break;
        case FATTREE:
            buildFatTreeTopo(nodeSet);
            break;
        default:
            cout<<"Unknown Topology Type.\n";
            break;
    }
}
