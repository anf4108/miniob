/* Copyright (c) 2021 OceanBase and/or its affiliates. All rights reserved.
miniob is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
         http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

//
// Created by Longda on 2010
//

#include <assert.h>
#include <errno.h>
#include <fstream>
#include <libgen.h>
#include <paths.h>
#include <sstream>
#include <string.h>
#include <unistd.h>

#include "common/log/log.h"
#include "common/os/pidfile.h"
#include "common/lang/iostream.h"
#include "common/lang/fstream.h"

namespace common {

string &getPidPath()
{
  static string path;

  return path;
}

void setPidPath(const char *progName)
{
  string &path = getPidPath();

  if (progName != NULL) {
    path = string(_PATH_TMP) + progName + ".pid";
  } else {
    path = "";
  }
}

int writePidFile(const char *progName)
{
  assert(progName);
  ofstream ostr;
  int      rv = 1;

  setPidPath(progName);
  string path = getPidPath();
  ostr.open(path.c_str(), ios::trunc);
  if (ostr.good()) {
    ostr << getpid() << endl;
    ostr.close();
    rv = 0;
  } else {
    rv = errno;
    cerr << "error opening PID file " << path.c_str() << SYS_OUTPUT_ERROR << endl;
  }

  return rv;
}

void removePidFile(void)
{
  string path = getPidPath();
  if (!path.empty()) {
    unlink(path.c_str());
    setPidPath(NULL);
  }
  return;
}

}  // namespace common