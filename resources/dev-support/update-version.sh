#!/bin/bash
#
# Copyright 2018 StreamSets Inc.
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
#

set -e
set -x
version=$1
if [[ -z "$version" ]]
then
  echo "Usage: $0 NEW-VERSION"
  exit 1
fi
sed -i.bak "s|^\(version*=*\).*|\1$version|" gradle.properties
if [ -f gradle.properties.bak ]; then
	rm -f gradle.properties.bak
fi
