#ifndef HADOOPSIM_NETSIM_MSGINFO_H_
#define HADOOPSIM_NETSIM_MSGINFO_H_
#include "netsim/defs.h"
#include <vector>
#include "netsim/topology.h"
namespace HadoopNetSim {

typedef uint64_t MsgId;
static const MsgId MsgId_invalid = UINT64_C(0);

enum MsgType {
  kMTNone,
  kMTNameRequest,
  kMTNameResponse,
  kMTDataRequest,
  kMTDataResponse,
  kMTSnmp,
  kMTImportRequest,
  kMTImportResponse
};

class MsgInfo;

typedef ns3::Callback<void,ns3::Ptr<MsgInfo>> TransmitCb;
static const TransmitCb TransmitCb_null = ns3::MakeNullCallback<void,ns3::Ptr<MsgInfo>>();

class MsgInfo : public ns3::SimpleRefCount<MsgInfo> {
  public:
    MsgInfo(void);
    virtual ~MsgInfo(void) {}
    MsgId id(void) const { return this->id_; }
    void set_id(MsgId value) { this->id_ = value; }
    MsgId in_reply_to(void) const { return this->in_reply_to_; }
    void set_in_reply_to(MsgId value) { this->in_reply_to_ = value; }
    MsgType type(void) const { return this->type_; }
    void set_type(MsgType value) { this->type_ = value; }
    HostName src(void) const { return this->pipeline_.front(); }
    HostName dst(void) const { return this->pipeline_.back(); }
    const std::vector<HostName>& pipeline(void) const { return this->pipeline_; }
    void set_pipeline(const std::vector<HostName>& value) { this->pipeline_ = value; }
    HostName FindNextHost(HostName localhost) const;
    void set_srcdst(HostName src, HostName dst);
    size_t size(void) const { return this->size_; }
    void set_size(size_t value) { this->size_ = value; }
    bool success(void) const { return this->success_; }
    void set_success(bool value) { this->success_ = value; }
    ns3::Time start(void) const { return this->start_; }
    void set_start(ns3::Time value) { this->start_ = value; }
    ns3::Time finish(void) const { return this->finish_; }
    void set_finish(ns3::Time value) { this->finish_ = value; }
    TransmitCb cb(void) const { return this->cb_; }
    void set_cb(TransmitCb value) { this->cb_ = value; }
    void invoke_cb(void) { if (!this->cb_.IsNull()) this->cb_(this); }
    void* userobj(void) const { return this->userobj_; }
    void set_userobj(void* value) { this->userobj_ = value; }
    
  private:
    MsgId id_;//message id
    MsgId in_reply_to_;//in reply to message id, applicable for kMTDataResponse and kMTImportResponse only
    MsgType type_;//message type
    std::vector<HostName> pipeline_;
    size_t size_;//payload size in octets
    bool success_;//whether transmission succeeds
    ns3::Time start_;//sending start time
    ns3::Time finish_;//receiving finish time
    TransmitCb cb_;//callback after receiving finish
    void* userobj_;//user object
    DISALLOW_COPY_AND_ASSIGN(MsgInfo);
};

};//namespace HadoopNetSim
#endif//HADOOPSIM_NETSIM_MSGINFO_H_
