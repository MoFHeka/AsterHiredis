/*
 * Copyright (c) 2022, Jia He <mofhejia@163.com>
 *
 * All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef MOFHEKA_ASTERHIREDIS_ASTERHIREDIS_H
#define MOFHEKA_ASTERHIREDIS_ASTERHIREDIS_H

#pragma once

#define ASTER_HIREDIS_MAJOR 0
#define ASTER_HIREDIS_MINOR 1
#define ASTER_HIREDIS_PATCH 0
#define ASTER_HIREDIS_SONAME 0.1.0-dev

#include "client_util.hpp"
#include "cluster.hh"
#include "sentinel.hh"
#include "standalone.hh"

#endif  // end MOFHEKA_ASTERHIREDIS_ASTERHIREDIS_H