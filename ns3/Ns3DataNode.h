/*
Lei Ye <leiy@cs.arizona.edu>
HadoopSim is a simulator for a Hadoop Runtime by replaying the collected traces.
*/
#ifndef NS3DATANODE_H
#define NS3DATANODE_H

#include "ns3/application-container.h"
#include "ns3/node-container.h"
#include "ns3/object-factory.h"
#include "ns3/application.h"
#include "ns3/event-id.h"
#include "ns3/ptr.h"
#include "ns3/ipv4-address.h"
#include "ns3/address.h"

namespace ns3 {

typedef struct DataRequest {
    uint32_t dataType;
    uint32_t dataRequestID;
    size_t requestBytes;
}DataRequest;

class Socket;
class Packet;

class DataNodeServer: public Application {
public:
    static TypeId GetTypeId(void);
    DataNodeServer();
    virtual ~DataNodeServer();
    void ReceivePacket(Ptr<Socket> socket);
    void HandleAccept(Ptr<Socket> s, const Address& from);
    void HandleSuccessClose(Ptr<Socket> s);
protected:
    virtual void DoDispose(void);
private:
    virtual void StartApplication(void);
    virtual void StopApplication(void);
    void HandleRead(Ptr<Socket> socket);
    Ptr<Socket> m_socket;
    uint16_t m_port;
    bool m_running;
};

class DataNodeClient: public Application {
public:
    static TypeId GetTypeId(void);
    DataNodeClient();
    virtual ~DataNodeClient();
    void SetRemote(Ipv4Address ip, uint16_t port);
    /**
     * Set the data size of the packet (the number of bytes that are sent as data
     * to the server).  The contents of the data are set to unspecified (don't
     * care) by this call. If you have set the fill data for the echo client using
     * one of the SetFill calls, this will undo those effects.
     */
    void SetDataSize(uint32_t dataSize);
    /**
     * Get the number of data bytes that will be sent to the server.
     *
     */
    uint32_t GetDataSize(void) const;
    /**
     * Set the data fill of the packet (what is sent as data to the server) to
     * the zero-terminated contents of the fill string string.
     */
    void SetFill(std::string fill);
    /**
     * Set the data fill of the packet (what is sent as data to the server) to
     * the repeated contents of the fill byte.  i.e., the fill byte will be
     * used to initialize the contents of the data packet. dataSize is the desired
     * size of the resulting echo packet data.
     */
    void SetFill(uint8_t fill, uint32_t dataSize);
    /**
     * Set the data fill of the packet (what is sent as data to the server) to
     * the contents of the fill buffer, repeated as many times as is required.
     * \param fill The fill pattern to use when constructing packets.
     * \param fillSize The number of bytes in the provided fill pattern.
     * \param dataSize The desired size of the final echo data.
     */
    void SetFill(uint8_t *fill, uint32_t fillSize, uint32_t dataSize);
    uint32_t GetPacketsSent(void) { return m_sent; };
    uint64_t GetBytesSent(void) { return m_bytesSent; };
    uint32_t GetPacketsReceivedBack(void) { return m_recvBack; };
    uint64_t GetBytesReceivedBack(void) { return m_bytesRecvBack; };
    void ScheduleTransmit(Time dt);
protected:
    virtual void DoDispose(void);
private:
    virtual void StartApplication(void);
    virtual void StopApplication(void);
    void Send(void);
    void ReceivePacket(Ptr<Socket> socket);
    uint32_t m_count;
    Time m_interval;
    uint32_t m_size;
    uint32_t m_dataSize;
    uint8_t *m_data;
    uint32_t m_sent;
    uint64_t m_bytesSent;
    uint32_t m_recvBack;
    uint64_t m_bytesRecvBack;
    Ptr<Socket> m_socket;
    Ipv4Address m_peerAddress;
    uint16_t m_peerPort;
    EventId m_sendEvent;
};

class DataNodeServerHelper {
public:
    DataNodeServerHelper(uint16_t port);
    /**
     * Record an attribute to be set in each Application after it is is created.
     */
    void SetAttribute(std::string name, const AttributeValue &value);
    /**
     * Create a NameNodeServerApplication on the specified Node.
     */
    ApplicationContainer Install(Ptr<Node> node) const;
    /**
     * Create a NameNodeServerApplication on specified node
     */
    ApplicationContainer Install(std::string nodeName) const;
    /**
     * \param c The nodes on which to create the Applications.  The nodes
     *          are specified by a NodeContainer.
     */
    ApplicationContainer Install(NodeContainer c) const;
private:
    Ptr<Application> InstallPriv(Ptr<Node> node) const;
    ObjectFactory m_factory;
};

class DataNodeClientHelper {
public:
    DataNodeClientHelper(Ipv4Address ip, uint16_t port);
    /**
     * Record an attribute to be set in each Application after it is created.
     */
    void SetAttribute(std::string name, const AttributeValue &value);
    /**
     * Given a pointer to a TcpEchoClient application, set the data fill of the TCP
     * packet (what is sent as data to the server) to the contents of the fill
     * string (including the trailing zero terminator).
     */
    void SetFill(Ptr<Application> app, std::string fill);
    /**
     * Given a pointer to a TcpEchoClient application, set the data fill of the
     * packet (what is sent as data to the server) to the contents of the fill
     * byte.
     */
    void SetFill(Ptr<Application> app, uint8_t fill, uint32_t dataLength);
    /**
     * Given a pointer to a TcpEchoClient application, set the data fill of the
     * packet (what is sent as data to the server) to the contents of the fill
     * buffer, repeated as many times as is required.
     */
    void SetFill(Ptr<Application> app, uint8_t *fill, uint32_t fillLength, uint32_t dataLength);
    /**
     * Create a TCP echo client application on the specified node.  The Node
     * is provided as a Ptr<Node>.
     */
    ApplicationContainer Install(Ptr<Node> node);
    /**
     * Create a TCP echo client application on the specified node.  The Node
     * is provided as a string name of a Node that has been previously
     * associated using the Object Name Service.
     */
    ApplicationContainer Install(std::string nodeName);
    /**
     * Create one TCP echo client application on each of the input nodes
     */
    ApplicationContainer Install(NodeContainer c);
    Ptr<DataNodeClient> GetClient(void);
private:
    Ptr<Application> InstallPriv(Ptr<Node> node);
    ObjectFactory m_factory;
    Ptr<DataNodeClient> m_client;
};

}

#endif // NS3DATANODE_H
