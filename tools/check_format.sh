#!/bin/sh
# Copyright 2023 Greptime Team
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.



# make sure git has no un commit files
if [ -n "$(git status --untracked-files=no --porcelain)" ]; then
   echo "Please commit your change before run this shell, un commit files:"
   git status --untracked-files=no --porcelain
   exit -1
fi
