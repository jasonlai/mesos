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
#include <utility>
#include <vector>

#include <glog/logging.h>

#include <stout/duration.hpp>
#include <stout/hashmap.hpp>
#include <stout/json.hpp>
#include <stout/os.hpp>

#include <process/collect.hpp>
#include <process/defer.hpp>
#include <process/dispatch.hpp>
#include <process/id.hpp>
#include <process/metrics/counter.hpp>
#include <process/metrics/gauge.hpp>
#include <process/metrics/metrics.hpp>
#include <process/metrics/timer.hpp>

#include <mesos/docker/spec.hpp>

#include "slave/containerizer/mesos/isolators/posix/disk.hpp"

#include "slave/containerizer/mesos/provisioner/constants.hpp"
#include "slave/containerizer/mesos/provisioner/utils.hpp"

#include "slave/containerizer/mesos/provisioner/docker/metadata_manager.hpp"
#include "slave/containerizer/mesos/provisioner/docker/paths.hpp"
#include "slave/containerizer/mesos/provisioner/docker/puller.hpp"
#include "slave/containerizer/mesos/provisioner/docker/store.hpp"

#include "uri/fetcher.hpp"

using namespace process;

namespace spec = docker::spec;

using std::list;
using std::pair;
using std::string;
using std::vector;

namespace mesos {
namespace internal {
namespace slave {
namespace docker {

class StoreProcess : public Process<StoreProcess>
{
public:
  StoreProcess(
      const Flags& _flags,
      const Owned<MetadataManager>& _metadataManager,
      const Owned<Puller>& _puller)
    : ProcessBase(process::ID::generate("docker-provisioner-store")),
      flags(_flags),
      metadataManager(_metadataManager),
      puller(_puller),
      collector(Seconds(0)),
      layers(
          "containerizer/mesos/docker_store/layers",
          defer(self(), &StoreProcess::_layers)),
      layersPulled("containerizer/mesos/docker_store/layers_pulled"),
      layersMb(
          "containerizer/mesos/docker_store/layers_mb",
          defer(self(), &StoreProcess::_layersMb)),
      pullTimer("containerizer/mesos/docker_store/pull", Hours(1))
  {
    process::metrics::add(layers);
    process::metrics::add(layersMb);
    process::metrics::add(layersPulled);
    process::metrics::add(pullTimer);
  }

  ~StoreProcess()
  {
    process::metrics::remove(layers);
    process::metrics::remove(layersMb);
    process::metrics::remove(layersPulled);
    process::metrics::remove(pullTimer);
  }

  Future<Nothing> recover();

  Future<ImageInfo> get(
      const mesos::Image& image,
      const string& backend);

private:
  Future<Nothing> _recover(const vector<Layer>& layers);

  Future<Image> _get(
      const spec::ImageReference& reference,
      const Option<pair<Image, vector<Layer>>>& imageLayers,
      const string& backend);

  Future<ImageInfo> __get(
      const Image& image,
      const string& backend);

  Future<vector<Layer>> moveLayers(
      const string& staging,
      const vector<string>& layerIds,
      const string& backend);

  Future<Layer> moveLayer(
      const string& staging,
      const string& layerId,
      const string& backend);

  Future<Layer> _moveLayer(
      const string& layerId,
      const Bytes& bytes);

  Future<Nothing> backfill(
      const string& layerId,
      const Bytes& bytes);

  Future<double> _layers();

  Future<double> _layersMb();

  const Flags flags;

  Owned<MetadataManager> metadataManager;
  Owned<Puller> puller;
  hashmap<string, Owned<Promise<Image>>> pulling;

  DiskUsageCollector collector;
  hashmap<string, Bytes> layerBytes;

  process::metrics::Gauge layers;
  process::metrics::Counter layersPulled;
  process::metrics::Gauge layersMb;
  process::metrics::Timer<Milliseconds> pullTimer;
};


Try<Owned<slave::Store>> Store::create(const Flags& flags)
{
  // TODO(jieyu): We should inject URI fetcher from top level, instead
  // of creating it here.
  uri::fetcher::Flags _flags;

  // TODO(dpravat): Remove after resolving MESOS-5473.
#ifndef __WINDOWS__
  _flags.docker_config = flags.docker_config;
#endif

  Try<Owned<uri::Fetcher>> fetcher = uri::fetcher::create(_flags);
  if (fetcher.isError()) {
    return Error("Failed to create the URI fetcher: " + fetcher.error());
  }

  Try<Owned<Puller>> puller = Puller::create(flags, fetcher->share());
  if (puller.isError()) {
    return Error("Failed to create Docker puller: " + puller.error());
  }

  Try<Owned<slave::Store>> store = Store::create(flags, puller.get());
  if (store.isError()) {
    return Error("Failed to create Docker store: " + store.error());
  }

  return store.get();
}


Try<Owned<slave::Store>> Store::create(
    const Flags& flags,
    const Owned<Puller>& puller)
{
  Try<Nothing> mkdir = os::mkdir(flags.docker_store_dir);
  if (mkdir.isError()) {
    return Error("Failed to create Docker store directory: " +
                 mkdir.error());
  }

  mkdir = os::mkdir(paths::getStagingDir(flags.docker_store_dir));
  if (mkdir.isError()) {
    return Error("Failed to create Docker store staging directory: " +
                 mkdir.error());
  }

  Try<Owned<MetadataManager>> metadataManager = MetadataManager::create(flags);
  if (metadataManager.isError()) {
    return Error(metadataManager.error());
  }

  Owned<StoreProcess> process(
      new StoreProcess(flags, metadataManager.get(), puller));

  return Owned<slave::Store>(new Store(process));
}


Store::Store(Owned<StoreProcess> _process) : process(_process)
{
  spawn(CHECK_NOTNULL(process.get()));
}


Store::~Store()
{
  terminate(process.get());
  wait(process.get());
}


Future<Nothing> Store::recover()
{
  return dispatch(process.get(), &StoreProcess::recover);
}


Future<ImageInfo> Store::get(
    const mesos::Image& image,
    const string& backend)
{
  return dispatch(process.get(), &StoreProcess::get, image, backend);
}


Future<Nothing> StoreProcess::recover()
{
  return metadataManager->recover()
    .then(defer(self(), &Self::_recover, lambda::_1));
}


Future<Nothing> StoreProcess::_recover(const vector<Layer>& layers)
{
  foreach(const Layer& layer, layers) {
    const string layerPath = paths::getImageLayerPath(
        flags.docker_store_dir,
        layer.id());

    if (!os::exists(layerPath)) {
      LOG(WARNING) << "Recovered layer path " << layerPath
                   << "does not exist";
      continue;
    }

    if (layer.has_bytes()) {
      layerBytes[layer.id()] = Bytes(layer.bytes());
    } else {
      LOG(INFO) << "Backfilling disk size for layer " << layer.id();

      collector.usage(layerPath, vector<string>())
          .then(defer(self(), &Self::backfill, layer.id(), lambda::_1));
      // Do not to block `recover()` progress, but only backfill
      // layer size after store is available.
    }
  }

  return Nothing();
}


Future<ImageInfo> StoreProcess::get(
    const mesos::Image& image,
    const string& backend)
{
  if (image.type() != mesos::Image::DOCKER) {
    return Failure("Docker provisioner store only supports Docker images");
  }

  Try<spec::ImageReference> reference =
    spec::parseImageReference(image.docker().name());

  if (reference.isError()) {
    return Failure("Failed to parse docker image '" + image.docker().name() +
                   "': " + reference.error());
  }

  return metadataManager->get(reference.get(), image.cached())
    .then(defer(self(), &Self::_get, reference.get(), lambda::_1, backend))
    .then(defer(self(), &Self::__get, lambda::_1, backend));
}


Future<Image> StoreProcess::_get(
    const spec::ImageReference& reference,
    const Option<pair<Image, vector<Layer>>>& imageLayers,
    const string& backend)
{
  // NOTE: Here, we assume that image layers are not removed without
  // first removing the metadata in the metadata manager first.
  // Otherwise, the image we return here might miss some layers. At
  // the time we introduce cache eviction, we also want to avoid the
  // situation where a layer was returned to the provisioner but is
  // later evicted.
  if (imageLayers.isSome()) {
    const Image& image = imageLayers.get().first;

    // It is possible that a layer is missed after recovery if the
    // agent flag `--image_provisioner_backend` is changed from a
    // specified backend to `None()`. We need to check that each
    // layer exists for a cached image.
    bool layerMissed = false;

    foreach (const string& layerId, image.layer_ids()) {
      const string rootfsPath = paths::getImageLayerRootfsPath(
          flags.docker_store_dir,
          layerId,
          backend);

      if (!os::exists(rootfsPath)) {
        layerMissed = true;
        break;
      }
    }

    if (!layerMissed) {
      return imageLayers.get().first;
    }
  }

  Try<string> staging =
    os::mkdtemp(paths::getStagingTempDir(flags.docker_store_dir));

  if (staging.isError()) {
    return Failure("Failed to create a staging directory: " + staging.error());
  }

  // If there is already an pulling going on for the given 'name', we
  // will skip the additional pulling.
  const string name = stringify(reference);

  if (!pulling.contains(name)) {
    Owned<Promise<Image>> promise(new Promise<Image>());

    Future<Image> future =
      pullTimer.time(puller->pull(reference, staging.get(), backend))
      .then(defer(self(), &Self::moveLayers, staging.get(), lambda::_1, backend))
      .then(defer(self(), [=](const vector<Layer>& layers) {
        return metadataManager->put(reference, layers);
      }))
      .onAny(defer(self(), [=](const Future<Image>&) {
        pulling.erase(name);

        Try<Nothing> rmdir = os::rmdir(staging.get());
        if (rmdir.isError()) {
          LOG(WARNING) << "Failed to remove staging directory: "
                       << rmdir.error();
        }
      }));

    promise->associate(future);
    pulling[name] = promise;

    return promise->future();
  }

  return pulling[name]->future();
}


Future<ImageInfo> StoreProcess::__get(
    const Image& image,
    const string& backend)
{
  CHECK_LT(0, image.layer_ids_size());

  vector<string> layerPaths;
  foreach (const string& layerId, image.layer_ids()) {
    layerPaths.push_back(paths::getImageLayerRootfsPath(
        flags.docker_store_dir,
        layerId,
        backend));
  }

  // Read the manifest from the last layer because all runtime config
  // are merged at the leaf already.
  Try<string> manifest = os::read(
      paths::getImageLayerManifestPath(
          flags.docker_store_dir,
          image.layer_ids(image.layer_ids_size() - 1)));

  if (manifest.isError()) {
    return Failure("Failed to read manifest: " + manifest.error());
  }

  Try<::docker::spec::v1::ImageManifest> v1 =
    ::docker::spec::v1::parse(manifest.get());

  if (v1.isError()) {
    return Failure("Failed to parse docker v1 manifest: " + v1.error());
  }

  return ImageInfo{layerPaths, v1.get()};
}


Future<vector<Layer>> StoreProcess::moveLayers(
    const string& staging,
    const vector<string>& layerIds,
    const string& backend)
{
  list<Future<Layer>> futures;
  foreach (const string& layerId, layerIds) {
    futures.push_back(moveLayer(staging, layerId, backend));
  }

  return collect(futures)
    .then([](const list<Layer>& layers) -> vector<Layer> {
      return { std::begin(layers), std::end(layers) };
    });
}


Future<Layer> StoreProcess::moveLayer(
    const string& staging,
    const string& layerId,
    const string& backend)
{
  Layer layer;
  layer.set_id(layerId);
  Option<Bytes> bytes = layerBytes.get(layerId);
  if (bytes.isSome()) {
    layer.set_bytes(bytes.get().bytes());
  }

  const string source = path::join(staging, layerId);

  // This is the case where the puller skips the pulling of the layer
  // because the layer already exists in the store.
  //
  // TODO(jieyu): Verify that the layer is actually in the store.
  if (!os::exists(source)) {
    return layer;
  }

  const string targetRootfs = paths::getImageLayerRootfsPath(
      flags.docker_store_dir,
      layerId,
      backend);

  // NOTE: Since the layer id is supposed to be unique. If the layer
  // already exists in the store, we'll skip the moving since they are
  // expected to be the same.
  if (os::exists(targetRootfs)) {
    return layer;
  }

  const string sourceRootfs = paths::getImageLayerRootfsPath(source, backend);
  const string target = paths::getImageLayerPath(
      flags.docker_store_dir,
      layerId);

#ifdef __linux__
  // If the backend is "overlay", we need to convert
  // AUFS whiteout files to OverlayFS whiteout files.
  if (backend == OVERLAY_BACKEND) {
    Try<Nothing> convert = convertWhiteouts(sourceRootfs);
    if (convert.isError()) {
      return Failure(
          "Failed to convert the whiteout files under '" +
          sourceRootfs + "': " + convert.error());
    }
  }
#endif

  if (!os::exists(target)) {
    // This is the case that we pull the layer for the first time.
    Try<Nothing> mkdir = os::mkdir(target);
    if (mkdir.isError()) {
      return Failure(
          "Failed to create directory in store for layer '" +
          layerId + "': " + mkdir.error());
    }

    Try<Nothing> rename = os::rename(source, target);
    if (rename.isError()) {
      return Failure(
          "Failed to move layer from '" + source +
          "' to '" + target + "': " + rename.error());
    }

    layersPulled++;
  } else {
    // This is the case where the layer has already been pulled with a
    // different backend.
    Try<Nothing> rename = os::rename(sourceRootfs, targetRootfs);
    if (rename.isError()) {
      return Failure(
          "Failed to move rootfs from '" + sourceRootfs +
          "' to '" + targetRootfs + "': " + rename.error());
    }
  }

  return collector.usage(target, vector<string>())
      .then(defer(self(), &Self::_moveLayer, layerId, lambda::_1));
}


Future<Layer> StoreProcess::_moveLayer(
    const string& layerId,
    const Bytes& bytes)
{
  layerBytes[layerId] = bytes;
  Layer layer;
  layer.set_id(layerId);
  layer.set_bytes(bytes.bytes());
  return layer;
}


Future<Nothing> StoreProcess::backfill(
    const string& layerId,
    const Bytes& bytes)
{
  layerBytes[layerId] = bytes;
  Layer layer;
  layer.set_id(layerId);
  layer.set_bytes(bytes.bytes());
  return metadataManager->putLayer(layer);
}


Future<double> StoreProcess::_layers()
{
  return layerBytes.size();
}


Future<double> StoreProcess::_layersMb()
{
  double result = 0;
  foreachvalue(const Bytes& bytes, layerBytes) {
    result += bytes.megabytes();
  }
  return result;
}

} // namespace docker {
} // namespace slave {
} // namespace internal {
} // namespace mesos {
