# Copyright 2017 Xiaomi, Inc.
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
# ==============================================================================

# -*- coding: utf-8 -*-

from vision.visionclient import VisionClient


class Credential:
  def __init__(self, galaxy_access_key=None, galaxy_key_secret=None):
    """
    :param galaxy_access_key: string
    user's access key id
    :param galaxy_key_secret: string
    user's secret access key
    :return : Credential object
    """
    self.galaxy_access_key = galaxy_access_key
    self.galaxy_key_secret = galaxy_key_secret


class ClientFactory:
  def __init__(self, credential):
    self.credential = credential

  def client(self, name=None, endpoint=None, https_enables=False):
    """
    :param name: string
      service name, such vision, voice, nlp etc, but now only support vision
    :param endpoint: string
      service cluster address
    :return: Object
      such as VisionClient
    """
    # TODO: no depend VisionClient directly ?
    if name == "vision":
      return VisionClient(self.credential, endpoint, https_enables)
    else:
      raise KeyError("the name must in the list as follow", ["vision"])
