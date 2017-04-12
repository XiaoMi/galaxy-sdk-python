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

from common.clientfactory import *
from vision.models import *

client_factory = ClientFactory(Credential(galaxy_access_key="YOU-AK", galaxy_key_secret="YOU-SK"))
vision_client = client_factory.client("vision", endpoint="cnbj2.vision.api.xiaomi.com", https_enables=False)

image = Image(uri="fds://cnbj2.fds.api.xiaomi.com/vision-test/test_img.jpg")
detect_faces_request = DetectFacesRequest(image=image)

faces_list = vision_client.detect_faces(detect_faces_request)
print faces_list

detect_labels_request = DetectLabelsRequest(image=image)
labels_list = vision_client.detect_labels(detect_labels_request)
print labels_list
