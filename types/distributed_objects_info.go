/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License")
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package types

type DistributedObjectInfo struct {
	name        string
	serviceName string
}

func (i *DistributedObjectInfo) Name() string {
	return i.name
}

func (i *DistributedObjectInfo) ServiceName() string {
	return i.serviceName
}

func NewDistributedObjectInfo(name string, serviceName string) DistributedObjectInfo {
	return DistributedObjectInfo{name: name, serviceName: serviceName}
}
