/*
Lei Ye <leiy@cs.arizona.edu>
HadoopSim is a simulator for a Hadoop Runtime by replaying the collected traces.
*/
#include <sstream>
#include "ns3/log.h"
#include "ns3/ipv4-address.h"
#include "ns3/address-utils.h"
#include "ns3/nstime.h"
#include "ns3/inet-socket-address.h"
#include "ns3/socket.h"
#include "ns3/simulator.h"
#include "ns3/socket-factory.h"
#include "ns3/packet.h"
#include "ns3/uinteger.h"
#include "ns3/names.h"
#include "Ns3NameNode.h"
#include "Ns3.h"
#include "../TaskTracker.h"
using namespace std;

namespace ns3 {

NS_LOG_COMPONENT_DEFINE("NameNodeApplication");
NS_OBJECT_ENSURE_REGISTERED(NameNodeServer);

TypeId NameNodeServer::GetTypeId(void)
{
    static TypeId tid = TypeId("ns3::NameNodeServer")
        .SetParent<Application>()
        .AddConstructor<NameNodeServer>()
        .AddAttribute("Port", "Port on which we listen for incoming connections.",
                   UintegerValue(2009),
                   MakeUintegerAccessor(&NameNodeServer::m_port),
                   MakeUintegerChecker<uint16_t>());
    return tid;
}

NameNodeServer::NameNodeServer()
{
    NS_LOG_FUNCTION_NOARGS();
    m_socket = 0;
    m_running = false;
}

NameNodeServer::~NameNodeServer()
{
    NS_LOG_FUNCTION_NOARGS();
    m_socket = 0;
}

void NameNodeServer::DoDispose(void)
{
    NS_LOG_FUNCTION_NOARGS();
    Application::DoDispose();
}

void NameNodeServer::ReceivePacket(Ptr<Socket> s)
{
    NS_LOG_FUNCTION(this << s);

    Ptr<Packet> packet;
    Address from;
    while(packet = s->RecvFrom(from)) {
        if (packet->GetSize() > 0) {
            NS_LOG_INFO("NameNodeServer Received HeartBeatReport " << packet->GetSize() << " bytes from " <<
                                   InetSocketAddress::ConvertFrom(from).GetIpv4());
            NS_LOG_LOGIC("Send back HeartBeatResponse");
            stringstream ss;
            ss<<InetSocketAddress::ConvertFrom(from).GetIpv4();
            uint32_t packetSize = reportArrive(ss.str());
            Ptr<Packet> p = Create<Packet>(packetSize);
            s->Send(p);
        }
    }
}

void NameNodeServer::HandleSuccessClose(Ptr<Socket> s)
{
    NS_LOG_FUNCTION(this << s);
    NS_LOG_LOGIC("Client close received");
    s->Close();
    s->SetRecvCallback(MakeNullCallback<void, Ptr<Socket> >());
    s->SetCloseCallbacks(MakeNullCallback<void, Ptr<Socket> >(),
                        MakeNullCallback<void, Ptr<Socket> >());
}

void NameNodeServer::HandleAccept(Ptr<Socket> s, const Address& from)
{
    NS_LOG_FUNCTION(this << s << from);
    s->SetRecvCallback(MakeCallback(&NameNodeServer::ReceivePacket, this));
    s->SetCloseCallbacks(MakeCallback(&NameNodeServer::HandleSuccessClose, this),
                        MakeNullCallback<void, Ptr<Socket> >());
}

void NameNodeServer::StartApplication(void)
{
    NS_LOG_FUNCTION_NOARGS();
    m_running = true;
    if (m_socket == 0) {
        TypeId tid = TypeId::LookupByName("ns3::TcpSocketFactory");
        m_socket = Socket::CreateSocket(GetNode(), tid);
        InetSocketAddress listenAddress = InetSocketAddress(Ipv4Address::GetAny (), m_port);
        m_socket->Bind(listenAddress);
        m_socket->Listen();
    }
    m_socket->SetAcceptCallback(
        MakeNullCallback<bool, Ptr<Socket>, const Address &>(),
        MakeCallback(&NameNodeServer::HandleAccept, this));
}

void NameNodeServer::StopApplication()
{
    NS_LOG_FUNCTION_NOARGS();
    m_running = false;
    if (m_socket != 0) {
        m_socket->Close();
        m_socket->SetAcceptCallback(
              MakeNullCallback<bool, Ptr<Socket>, const Address &>(),
              MakeNullCallback<void, Ptr<Socket>, const Address &>());
    }
}

NameNodeServerHelper::NameNodeServerHelper(uint16_t port)
{
    m_factory.SetTypeId(NameNodeServer::GetTypeId());
    SetAttribute("Port", UintegerValue(port));
}

void NameNodeServerHelper::SetAttribute(std::string name, const AttributeValue &value)
{
    m_factory.Set(name, value);
}

ApplicationContainer NameNodeServerHelper::Install(Ptr<Node> node) const
{
    return ApplicationContainer(InstallPriv(node));
}

ApplicationContainer NameNodeServerHelper::Install(std::string nodeName) const
{
    Ptr<Node> node = Names::Find<Node>(nodeName);
    return ApplicationContainer(InstallPriv(node));
}

ApplicationContainer NameNodeServerHelper::Install(NodeContainer c) const
{
    ApplicationContainer apps;
    for(NodeContainer::Iterator i = c.Begin(); i != c.End(); ++i)
        apps.Add(InstallPriv(*i));
    return apps;
}

Ptr<Application> NameNodeServerHelper::InstallPriv(Ptr<Node> node) const
{
    Ptr<Application> app = m_factory.Create<NameNodeServer>();
    node->AddApplication(app);
    return app;
}

////////////////////////////////////////////////////////////////////////////////
NS_OBJECT_ENSURE_REGISTERED(NameNodeClient);

TypeId NameNodeClient::GetTypeId(void)
{
    static TypeId tid = TypeId("ns3::NameNodeClient")
        .SetParent<Application>()
        .AddConstructor<NameNodeClient>()
        .AddAttribute("MaxPackets",
                      "The maximum number of packets the application will send",
                      UintegerValue(100),
                      MakeUintegerAccessor(&NameNodeClient::m_count),
                      MakeUintegerChecker<uint32_t>())
        .AddAttribute("Interval",
                      "The time to wait between packets",
                      TimeValue(Seconds(1.0)),
                      MakeTimeAccessor(&NameNodeClient::m_interval),
                      MakeTimeChecker())
        .AddAttribute("RemoteAddress",
                      "The destination Ipv4Address of the outbound packets",
                      Ipv4AddressValue(),
                      MakeIpv4AddressAccessor(&NameNodeClient::m_peerAddress),
                      MakeIpv4AddressChecker())
        .AddAttribute("RemotePort",
                      "The destination port of the outbound packets",
                      UintegerValue(0),
                      MakeUintegerAccessor(&NameNodeClient::m_peerPort),
                      MakeUintegerChecker<uint16_t>())
        .AddAttribute("PacketSize", "Size of echo data in outbound packets",
                      UintegerValue(100),
                      MakeUintegerAccessor(&NameNodeClient::SetDataSize,
                                           &NameNodeClient::GetDataSize),
                   MakeUintegerChecker<uint32_t>());
    return tid;
}

NameNodeClient::NameNodeClient()
{
    NS_LOG_FUNCTION_NOARGS();
    m_sent = 0;
    m_bytesSent = 0;
    m_recvBack = 0;
    m_bytesRecvBack = 0;
    m_socket = 0;
    m_sendEvent = EventId();
    m_data = 0;
    m_dataSize = 0;
}

NameNodeClient::~NameNodeClient()
{
    NS_LOG_FUNCTION_NOARGS();
    m_socket = 0;
    delete [] m_data;
    m_data = 0;
    m_dataSize = 0;
}

void NameNodeClient::SetRemote(Ipv4Address ip, uint16_t port)
{
    m_peerAddress = ip;
    m_peerPort = port;
}

void NameNodeClient::DoDispose(void)
{
    NS_LOG_FUNCTION_NOARGS();
    Application::DoDispose();
}

void NameNodeClient::StartApplication(void)
{
    NS_LOG_FUNCTION_NOARGS();
    if (m_socket == 0) {
        TypeId tid = TypeId::LookupByName("ns3::TcpSocketFactory");
        m_socket = Socket::CreateSocket(GetNode(), tid);
        m_socket->Bind();
        m_socket->Connect(InetSocketAddress(m_peerAddress, m_peerPort));
    }
    m_socket->SetRecvCallback(MakeCallback(&NameNodeClient::ReceivePacket, this));
//    ScheduleTransmit(Seconds (0.));
}

void NameNodeClient::StopApplication()
{
    NS_LOG_FUNCTION_NOARGS();
    if (m_socket != 0) {
        m_socket->Close();
        m_socket->SetRecvCallback(MakeNullCallback<void, Ptr<Socket> >());
        m_socket = 0;
    }
    Simulator::Cancel(m_sendEvent);
}

void NameNodeClient::SetDataSize(uint32_t dataSize)
{
    NS_LOG_FUNCTION(dataSize);
    // If the client is setting the echo packet data size this way, we infer
    // that she doesn't care about the contents of the packet at all, so neither will we.
    delete [] m_data;
    m_data = 0;
    m_dataSize = 0;
    m_size = dataSize;
}

uint32_t NameNodeClient::GetDataSize(void) const
{
    NS_LOG_FUNCTION_NOARGS();
    return m_size;
}

void NameNodeClient::SetFill(std::string fill)
{
    NS_LOG_FUNCTION(fill);
    uint32_t dataSize = fill.size() + 1;
    if (dataSize != m_dataSize) {
        delete [] m_data;
        m_data = new uint8_t [dataSize];
        m_dataSize = dataSize;
    }
    memcpy(m_data, fill.c_str(), dataSize);
    // Overwrite packet size attribute.
    m_size = dataSize;
}

void NameNodeClient::SetFill(uint8_t fill, uint32_t dataSize)
{
    if (dataSize != m_dataSize) {
        delete [] m_data;
        m_data = new uint8_t [dataSize];
        m_dataSize = dataSize;
    }
    memset(m_data, fill, dataSize);
    // Overwrite packet size attribute.
    m_size = dataSize;
}

void NameNodeClient::SetFill(uint8_t *fill, uint32_t fillSize, uint32_t dataSize)
{
    if (dataSize != m_dataSize) {
        delete [] m_data;
        m_data = new uint8_t [dataSize];
        m_dataSize = dataSize;
    }
    if (fillSize >= dataSize) {
        memcpy(m_data, fill, dataSize);
        return;
    }

    // Do all but the final fill.
    uint32_t filled = 0;
    while(filled + fillSize < dataSize) {
        memcpy(&m_data[filled], fill, fillSize);
        filled += fillSize;
    }
    // Last fill may be partial
    memcpy(&m_data[filled], fill, dataSize - filled);
    // Overwrite packet size attribute.
    m_size = dataSize;
}

void NameNodeClient::ScheduleTransmit(Time dt)
{
    NS_LOG_FUNCTION_NOARGS();
    m_sendEvent = Simulator::Schedule(dt, &NameNodeClient::Send, this);
}

void NameNodeClient::Send(void)
{
    NS_LOG_FUNCTION_NOARGS();
    NS_ASSERT (m_sendEvent.IsExpired ());

    Ptr<Packet> p;
    if (m_dataSize) {
        // If m_dataSize is non-zero, we have a data buffer of the same size that we
        // are expected to copy and send.  This state of affairs is created if one of
        // the Fill functions is called.  In this case, m_size must have been set
        // to agree with m_dataSize
        NS_ASSERT_MSG(m_dataSize == m_size, "NameNodeClient::Send(): m_size and m_dataSize inconsistent");
        NS_ASSERT_MSG(m_data, "NameNodeClient::Send(): m_dataSize but no m_data");
        p = Create<Packet>(m_data, m_dataSize);
        m_bytesSent += m_dataSize;
    } else {
        // If m_dataSize is zero, the client has indicated that she doesn't care
        // about the data itself either by specifying the data size by setting
        // the corresponding atribute or by not calling a SetFill function.  In
        // this case, we don't worry about it either.  But we do allow m_size
        // to have a value different from the (zero) m_dataSize.
        //
        p = Create<Packet>(m_size);
        m_bytesSent += m_size;
    }
    m_socket->Send(p);
    ++m_sent;

    stringstream ss;
    ss<<GetNode()->GetObject<Ipv4>()->GetAddress(1, 0).GetLocal();
    NS_LOG_INFO(ss.str() << " Sent " << m_size << " bytes to " << m_peerAddress);
    if (m_sent < m_count)
        ScheduleTransmit(m_interval);
}

void NameNodeClient::ReceivePacket(Ptr<Socket> socket)
{
    NS_LOG_FUNCTION(this << socket);
    Ptr<Packet> packet;
    Address from;
    while(packet = socket->RecvFrom(from)) {
        stringstream ss;
        ss<<GetNode()->GetObject<Ipv4>()->GetAddress(1, 0).GetLocal();
        NS_LOG_INFO (ss.str() << " Received " << packet->GetSize() << " bytes from " <<
                     InetSocketAddress::ConvertFrom(from).GetIpv4());
        // dont check if data returned is the same data sent earlier
        m_recvBack++;
        m_bytesRecvBack += packet->GetSize();

        responseArrive(ss.str());
    }
    if (m_count == m_recvBack) {
//        socket->Close();
//        m_socket->SetRecvCallback(MakeNullCallback<void, Ptr<Socket> >());
//        socket = 0;
    }
}

NameNodeClientHelper::NameNodeClientHelper(Ipv4Address address, uint16_t port)
{
    m_factory.SetTypeId(NameNodeClient::GetTypeId());
    SetAttribute("RemoteAddress", Ipv4AddressValue(address));
    SetAttribute("RemotePort", UintegerValue(port));
}

void NameNodeClientHelper::SetAttribute(std::string name, const AttributeValue &value)
{
    m_factory.Set(name, value);
}

void NameNodeClientHelper::SetFill(Ptr<Application> app, std::string fill)
{
    app->GetObject<NameNodeClient>()->SetFill(fill);
}

void NameNodeClientHelper::SetFill(Ptr<Application> app, uint8_t fill, uint32_t dataLength)
{
    app->GetObject<NameNodeClient>()->SetFill(fill, dataLength);
}

void NameNodeClientHelper::SetFill(Ptr<Application> app, uint8_t *fill, uint32_t fillLength, uint32_t dataLength)
{
    app->GetObject<NameNodeClient>()->SetFill(fill, fillLength, dataLength);
}

ApplicationContainer NameNodeClientHelper::Install(Ptr<Node> node)
{
    return ApplicationContainer(InstallPriv(node));
}

ApplicationContainer NameNodeClientHelper::Install(std::string nodeName)
{
    Ptr<Node> node = Names::Find<Node>(nodeName);
    return ApplicationContainer(InstallPriv(node));
}

ApplicationContainer NameNodeClientHelper::Install(NodeContainer c)
{
    ApplicationContainer apps;
    for(NodeContainer::Iterator i = c.Begin(); i != c.End(); ++i) {
        Ptr<Node> node = *i;
        m_client = m_factory.Create<NameNodeClient>();
        node->AddApplication(m_client);
        apps.Add(m_client);
    }
    return apps;
}

Ptr<Application> NameNodeClientHelper::InstallPriv(Ptr<Node> node)
{
    m_client = m_factory.Create<NameNodeClient>();
    node->AddApplication(m_client);
    return m_client;
}

Ptr<NameNodeClient> NameNodeClientHelper::GetClient(void)
{
    return m_client;
}

}
