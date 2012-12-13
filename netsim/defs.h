#ifndef HADOOPSIM_NETSIM_DEFS_H_
#define HADOOPSIM_NETSIM_DEFS_H_

#define __STDC_LIMIT_MACROS

#include <assert.h>
#include <cstddef>
#include <cinttypes>
#include <string>
#include <type_traits>

// A macro to disallow the copy constructor and operator= functions
// This should be used in the private: declarations for a class
#define DISALLOW_COPY_AND_ASSIGN(TypeName) \
  TypeName(const TypeName&);               \
  void operator=(const TypeName&)

#include "ns3/core-module.h"
#include "ns3/network-module.h"
//#include "ns3/csma-module.h"
#include "ns3/point-to-point-module.h"
//#include "ns3/bridge-module.h"
#include "ns3/internet-module.h"
#include "ns3/applications-module.h"
#include "ns3/ipv4-global-routing-helper.h"

namespace std {
template <typename T> struct hash<ns3::Ptr<T>> {
  size_t operator()(ns3::Ptr<T> ptr) const { std::hash<T*> hasher; return hasher(ns3::PeekPointer(ptr)); }
};
};

#endif//HADOOPSIM_NETSIM_DEFS_H_
