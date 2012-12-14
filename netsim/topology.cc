#include "netsim/topology.h"
#include <fstream>
namespace HadoopNetSim {

Node::Node(void) {
  this->type_ = kNTNone;
}

void Node::FromJson(::json_value* o) {
  assert(o != NULL);
  assert(o->type == JSON_OBJECT);

  ::json_value* p_type = NULL;
  ::json_value* p_ip = NULL;
  ::json_value* p_devices = NULL;
  for (::json_value* p = o->first_child; p != NULL; p = p->next_sibling) {
    std::string key = p->name;
    if (0 == key.compare("type")) {
      p_type = p;
    } else if (0 == key.compare("ip")) {
      p_ip = p;
    } else if (0 == key.compare("devices")) {
      p_devices = p;
    }
  }

  this->name_ = o->name;

  assert(p_type != NULL);
  assert(p_type->type == JSON_STRING);
  std::string v_type = p_type->string_value;
  if (0 == v_type.compare("host")) {
    this->type_ = kNTHost;
  } else if (0 == v_type.compare("switch")) {
    this->type_ = kNTSwitch;
  } else {
    assert(false);//unknown node type
  }

  if (p_ip != NULL) {
    assert(p_ip->type == JSON_STRING);
    this->ip_ = ns3::Ipv4Address(p_ip->string_value);
  } else {
    this->ip_ = ns3::Ipv4Address::GetAny();
  }

  assert(p_devices != NULL);
  assert(p_devices->type == JSON_ARRAY);
  this->devices_.clear();
  for (::json_value* p_device = p_devices->first_child; p_device != NULL; p_device = p_device->next_sibling) {
    assert(p_device->type == JSON_STRING);
    this->devices_.insert(p_device->string_value);
  }
}

Link::Link(void) {
  this->id_ = LinkId_invalid;
  this->type_ = kLTNone;
}

void Link::FromJson(::json_value* o) {
  assert(o != NULL);
  assert(o->type == JSON_OBJECT);

  ::json_value* p_node1 = NULL;
  ::json_value* p_port1 = NULL;
  ::json_value* p_node2 = NULL;
  ::json_value* p_port2 = NULL;
  ::json_value* p_type = NULL;
  for (::json_value* p = o->first_child; p != NULL; p = p->next_sibling) {
    std::string key = p->name;
    if (0 == key.compare("node1")) {
      p_node1 = p;
    } else if (0 == key.compare("port1")) {
      p_port1 = p;
    } else if (0 == key.compare("node2")) {
      p_node2 = p;
    } else if (0 == key.compare("port2")) {
      p_port2 = p;
    } else if (0 == key.compare("type")) {
      p_type = p;
    }
  }

  this->id_ = (LinkId)::atol(o->name);
  assert(this->id_ > 0);

  assert(p_node1 != NULL);
  assert(p_node1->type == JSON_STRING);
  this->node1_ = p_node1->string_value;

  assert(p_port1 != NULL);
  assert(p_port1->type == JSON_STRING);
  this->port1_ = p_port1->string_value;

  assert(p_node2 != NULL);
  assert(p_node2->type == JSON_STRING);
  this->node2_ = p_node2->string_value;

  assert(p_port2 != NULL);
  assert(p_port2->type == JSON_STRING);
  this->port2_ = p_port2->string_value;

  assert(p_type != NULL);
  assert(p_type->type == JSON_STRING);
  std::string v_type = p_type->string_value;
  if (0 == v_type.compare("eth1G")) {
    this->type_ = kLTEth1G;
  } else if (0 == v_type.compare("eth10G")) {
    this->type_ = kLTEth10G;
  } else {
    assert(false);//unknown link type
  }
}

void Topology::Load(const std::string& filename) {
  std::ifstream file(filename.c_str(), std::ios::binary);
  assert(file.is_open());
  file.seekg(0, std::ios::end);
  int len = file.tellg();
  file.seekg(0, std::ios::beg);

  char* buf = new char[len + 1];
  buf[len] = '\0';
  file.read(buf, len);
  file.close();

  this->LoadString(buf);

  delete[] buf;
}

void Topology::LoadString(char* json) {
  ::block_allocator ba(1024);
  char* error_pos; char* error_desc; int error_line;
  json_value* root = json_parse(json, &error_pos, &error_desc, &error_line, &ba);
  assert(root != NULL);
  assert(root->type == JSON_OBJECT);

  ::json_value* p_version = NULL;
  ::json_value* p_type = NULL;
  ::json_value* p_nodes = NULL;
  ::json_value* p_links = NULL;
  for (::json_value* p = root->first_child; p != NULL; p = p->next_sibling) {
    std::string key = p->name;
    if (0 == key.compare("version")) {
      p_version = p;
    } else if (0 == key.compare("type")) {
      p_type = p;
    } else if (0 == key.compare("nodes")) {
      p_nodes = p;
    } else if (0 == key.compare("links")) {
      p_links = p;
    }
  }

  assert(p_version != NULL);
  assert(p_version->type == JSON_INT);
  assert(p_version->int_value == 1);

  assert(p_type != NULL);
  assert(p_type->type == JSON_STRING);
  this->topotype_.assign(p_type->string_value);

  assert(p_nodes != NULL);
  assert(p_nodes->type == JSON_OBJECT);
  this->nodes_.clear();
  for (::json_value* p_node = p_nodes->first_child; p_node != NULL; p_node = p_node->next_sibling) {
    ns3::Ptr<Node> node = ns3::Create<Node>();
    node->FromJson(p_node);
    this->nodes_[node->name()] = node;
  }

  assert(p_links != NULL);
  assert(p_links->type == JSON_OBJECT);
  this->links_.clear();
  for (::json_value* p_link = p_links->first_child; p_link != NULL; p_link = p_link->next_sibling) {
    ns3::Ptr<Link> link = ns3::Create<Link>();
    link->FromJson(p_link);
    assert(this->nodes_.at(link->node1())->devices().count(link->port1()) == 1);
    assert(this->nodes_.at(link->node2())->devices().count(link->port2()) == 1);
    this->links_[link->id()] = link;
  }
}


};//namespace HadoopNetSim
