#ifndef HADOOPSIM_NETSIM_DEFS_H_
#define HADOOPSIM_NETSIM_DEFS_H_

#define __STDC_LIMIT_MACROS

#include <assert.h>
#include <cstddef>
#include <cinttypes>
#include <string>

// A macro to disallow the copy constructor and operator= functions
// This should be used in the private: declarations for a class
#define DISALLOW_COPY_AND_ASSIGN(TypeName) \
  TypeName(const TypeName&);               \
  void operator=(const TypeName&)

#endif//HADOOPSIM_NETSIM_DEFS_H_
