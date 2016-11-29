// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#ifndef __PROVISIONER_DOCKER_METADATA_MANAGER_HPP__
#define __PROVISIONER_DOCKER_METADATA_MANAGER_HPP__

#include <list>
#include <string>
#include <utility>

#include <stout/hashmap.hpp>
#include <stout/json.hpp>
#include <stout/option.hpp>
#include <stout/protobuf.hpp>
#include <stout/try.hpp>

#include <process/future.hpp>
#include <process/owned.hpp>
#include <process/process.hpp>

#include <mesos/docker/spec.hpp>

#include "slave/containerizer/mesos/provisioner/provisioner.hpp"

#include "slave/containerizer/mesos/provisioner/docker/message.hpp"

#include "slave/flags.hpp"

namespace mesos {
namespace internal {
namespace slave {
namespace docker {

// Forward Declaration.
class MetadataManagerProcess;

/**
 * The MetadataManager tracks the Docker images cached by the
 * provisioner that are stored on disk. It keeps track of the layers
 * that Docker images are composed of and recovers Image objects
 * upon initialization by checking for dependent layers stored on disk.
 * Currently, image layers are stored indefinitely, with no garbage
 * collection of unreferenced image layers.
 */
class MetadataManager
{
public:
  static Try<process::Owned<MetadataManager>> create(const Flags& flags);

  ~MetadataManager();

  /**
   * Recover all stored Image and its layer references, and return all
   * recovered layers upon success.
   */
  process::Future<std::vector<Layer>> recover();

  /**
   * Create an Image, put it in metadata manager and persist the reference
   * store state to disk.
   *
   * @param reference the reference of the Docker image to place in the
   *                  reference store.
   * @param layers the list of layers that comprise the Docker image in
   *               order where the root layer (no parent layer) is first
   *               and the leaf layer is last.
   */
  process::Future<Image> put(
      const ::docker::spec::ImageReference& reference,
      const std::vector<Layer>& layers);

  /**
   * Retrieve Image based on image reference if it is among the Images
   * stored in memory.
   *
   * @param reference the reference of the Docker image to retrieve
   * @param cached the flag whether pull Docker image forcelly from remote
   *               registry or local repo.
   */
  process::Future<Option<std::pair<Image, std::vector<Layer>>>> get(
      const ::docker::spec::ImageReference& reference,
      bool cached);

  /**
   * Store a layer. This is only used for backfilling layer size
   * after agent recovers unknown layers.
   *
   * @param layer the layer to store.
   */
  process::Future<Nothing> putLayer(const Layer& layer);

private:
  explicit MetadataManager(process::Owned<MetadataManagerProcess> process);

  MetadataManager(const MetadataManager&); // Not copyable.
  MetadataManager& operator=(const MetadataManager&); // Not assignable.

  process::Owned<MetadataManagerProcess> process;
};


} // namespace docker {
} // namespace slave {
} // namespace internal {
} // namespace mesos {

#endif // __PROVISIONER_DOCKER_METADATA_MANAGER_HPP__
