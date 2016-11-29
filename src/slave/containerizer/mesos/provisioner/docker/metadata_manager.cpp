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

#include <string>
#include <vector>
#include <utility>

#include <glog/logging.h>

#include <stout/foreach.hpp>
#include <stout/hashset.hpp>
#include <stout/os.hpp>
#include <stout/protobuf.hpp>

#include <process/defer.hpp>
#include <process/dispatch.hpp>
#include <process/owned.hpp>

#include "common/status_utils.hpp"

#include "slave/state.hpp"

#include "slave/containerizer/mesos/provisioner/docker/paths.hpp"
#include "slave/containerizer/mesos/provisioner/docker/message.hpp"
#include "slave/containerizer/mesos/provisioner/docker/metadata_manager.hpp"

namespace spec = docker::spec;

using std::list;
using std::pair;
using std::string;
using std::vector;

using process::Failure;
using process::Future;
using process::Owned;

namespace mesos {
namespace internal {
namespace slave {
namespace docker {

class MetadataManagerProcess : public process::Process<MetadataManagerProcess>
{
public:
  MetadataManagerProcess(const Flags& _flags) : flags(_flags) {}

  ~MetadataManagerProcess() {}

  Future<vector<Layer>> recover();

  Future<Image> put(
      const spec::ImageReference& reference,
      const vector<Layer>& layers);

  Future<Option<pair<Image, vector<Layer>>>> get(
      const spec::ImageReference& reference,
      bool cached);

  Future<Nothing> putLayer(const Layer& layer);

  // TODO(chenlily): Implement removal of unreferenced images.

private:
  // Write out metadata manager state to persistent store.
  Try<Nothing> persist();

  const Flags flags;

  // This is a lookup table for images that are stored in memory. It is keyed
  // by image name.
  // For example, "ubuntu:14.04" -> ubuntu14:04 Image.
  hashmap<string, Image> storedImages;

  // This is a lookup table for layers that are stored in memory.
  // It is keyed by layer id.
  hashmap<string, Layer> storedLayers;
};


Try<Owned<MetadataManager>> MetadataManager::create(const Flags& flags)
{
  Owned<MetadataManagerProcess> process(new MetadataManagerProcess(flags));

  return Owned<MetadataManager>(new MetadataManager(process));
}


MetadataManager::MetadataManager(Owned<MetadataManagerProcess> process)
  : process(process)
{
  spawn(CHECK_NOTNULL(process.get()));
}


MetadataManager::~MetadataManager()
{
  terminate(process.get());
  wait(process.get());
}


Future<vector<Layer>> MetadataManager::recover()
{
  return dispatch(process.get(), &MetadataManagerProcess::recover);
}


Future<Image> MetadataManager::put(
    const spec::ImageReference& reference,
    const vector<Layer>& layers)
{
  return dispatch(
      process.get(),
      &MetadataManagerProcess::put,
      reference,
      layers);
}


Future<Option<pair<Image, vector<Layer>>>> MetadataManager::get(
    const spec::ImageReference& reference,
    bool cached)
{
  return dispatch(
      process.get(),
      &MetadataManagerProcess::get,
      reference,
      cached);
}


Future<Nothing> MetadataManager::putLayer(
    const Layer& layer)
{
  return dispatch(
      process.get(),
      &MetadataManagerProcess::putLayer,
      layer);
}


Future<Image> MetadataManagerProcess::put(
    const spec::ImageReference& reference,
    const vector<Layer>& layers)
{
  const string imageReference = stringify(reference);

  Image dockerImage;
  dockerImage.mutable_reference()->CopyFrom(reference);
  foreach (const Layer& layer, layers) {
    dockerImage.add_layer_ids(layer.id());
    storedLayers[layer.id()] = layer;
  }

  storedImages[imageReference] = dockerImage;

  Try<Nothing> status = persist();
  if (status.isError()) {
    return Failure("Failed to save state of Docker images: " + status.error());
  }

  VLOG(1) << "Successfully cached image '" << imageReference << "'";

  return dockerImage;
}


Future<Option<pair<Image, vector<Layer>>>> MetadataManagerProcess::get(
    const spec::ImageReference& reference,
    bool cached)
{
  const string imageReference = stringify(reference);

  VLOG(1) << "Looking for image '" << imageReference << "'";

  if (!storedImages.contains(imageReference)) {
    return None();
  }

  if (!cached) {
    VLOG(1) << "Ignored cached image '" << imageReference << "'";
    return None();
  }

  Image image = storedImages[imageReference];
  vector<Layer> layers;
  layers.reserve(image.layer_ids_size());

  foreach (const string& layerId, image.layer_ids()) {
    Option<Layer> layer = storedLayers.get(layerId);

    if (layer.isNone()) {
      LOG(WARNING) << "Stored image reference ''" << imageReference
                   << "' has missing layer '" << layerId << "'.";
      return None();
    }

    layers.push_back(layer.get());
  }

  return std::make_pair(image, layers);
}


Future<Nothing> MetadataManagerProcess::putLayer(
    const Layer& layer)
{
  storedLayers[layer.id()] = layer;

  Try<Nothing> status = persist();
  if (status.isError()) {
    return Failure("Failed to save state of Docker images: " + status.error());
  }

  return Nothing();
}


Try<Nothing> MetadataManagerProcess::persist()
{
  Images images;

  foreachvalue (const Image& image, storedImages) {
    images.add_images()->CopyFrom(image);
  }

  foreachvalue (const Layer& layer, storedLayers) {
    images.add_layers()->CopyFrom(layer);
  }

  Try<Nothing> status = state::checkpoint(
      paths::getStoredImagesPath(flags.docker_store_dir), images);
  if (status.isError()) {
    return Error("Failed to perform checkpoint: " + status.error());
  }

  return Nothing();
}


Future<vector<Layer>> MetadataManagerProcess::recover()
{
  vector<Layer> layers;
  string storedImagesPath = paths::getStoredImagesPath(flags.docker_store_dir);

  if (!os::exists(storedImagesPath)) {
    LOG(INFO) << "No images to load from disk. Docker provisioner image "
              << "storage path '" << storedImagesPath << "' does not exist";
    return layers;
  }

  Result<Images> images = ::protobuf::read<Images>(storedImagesPath);
  if (images.isError()) {
    return Failure("Failed to read images from '" + storedImagesPath + "' " +
                   images.error());
  }

  if (images.isNone()) {
    // This could happen if the slave died after opening the file for
    // writing but before persisted on disk.
    return Failure("Unexpected empty images file '" + storedImagesPath + "'");
  }

  std::set<string> missingLayerIds;

  foreach (const Layer& layer, images.get().layers()) {
    const string& layerId = layer.id();

    const string imageLayerPath = paths::getImageLayerPath(
        flags.docker_store_dir,
        layerId);

    if (!os::exists(imageLayerPath)) {
      missingLayerIds.insert(layerId);
      LOG(WARNING) << "Skipped loading missing layer '" << layerId << "'";
      continue;
    }

    storedLayers[layerId] = layer;
  }

  foreach (const Image& image, images.get().images()) {
    const string imageReference = stringify(image.reference());

    bool missingLayer = false;

    foreach (const string& layerId, image.layer_ids()) {
      if (!storedLayers.contains(layerId)) {
        missingLayer = true;
        break;
      }
    }

    // TODO(zhitao): This will prevent any image to be recovered during upgrade
    // if the layer has not been previously persisted. This can be improved by
    // backfill their sizes once at recovery.
    if (missingLayer) {
      LOG(WARNING) << "Skipped loading image '" << imageReference << "'";
      continue;
    }

    if (storedImages.contains(imageReference)) {
      LOG(WARNING) << "Found duplicate image in recovery for image reference '"
                   << imageReference << "'";
    } else {
      storedImages[imageReference] = image;
    }

    VLOG(1) << "Successfully loaded image '" << imageReference << "'";
  }

  LOG(INFO) << "Successfully loaded " << storedImages.size()
            << " Docker images";

  layers.reserve(storedLayers.size());

  foreachvalue(const Layer& layer, storedLayers) {
    layers.push_back(layer);
  }

  return layers;
}

} // namespace docker {
} // namespace slave {
} // namespace internal {
} // namespace mesos {
