// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#ifndef __STOUT_OS_POSIX_REALPATH_HPP__
#define __STOUT_OS_POSIX_REALPATH_HPP__

#include <sys/stat.h>

#include <sstream>
#include <string>

#include <stout/error.hpp>
#include <stout/path.hpp>
#include <stout/result.hpp>

#include <stout/os/posix/readlink.hpp>


namespace os {

// Max symbolic links to follow.
static constexpr int MAX_SYMLINKS = 255;

inline Result<std::string> realpath(const std::string& path)
{
  char temp[PATH_MAX];
  if (::realpath(path.c_str(), temp) == nullptr) {
    if (errno == ENOENT || errno == ENOTDIR) {
      return None();
    }

    return ErrnoError();
  }

  return std::string(temp);
}

// Returns the real path of a given `path` scoped inside the provided `root`.
// This function follows any symbolic links and evaluates them within the
// provided `root` as the root of the filesystem, similar to chroot.
inline Result<std::string> realpath(
    const std::string& path,
    const std::string& root)
{
  const std::string separator(1, os::PATH_SEPARATOR);

  if (root == separator) {
    return realpath(path);
  }

  std::ostringstream buffer(separator);
  std::string unprocessed(path);
  int symlinks = 0;

  while (!unprocessed.empty()) {
    if (symlinks > MAX_SYMLINKS) {
      return Error("Too many symlinks followed");
    }

    const size_t found = unprocessed.find(separator);
    std::string component;

    if (found == std::string::npos) {
      component = unprocessed;
      unprocessed = "";
    } else {
      component = unprocessed.substr(0, found);
      unprocessed.erase(0, found + 1);
    }

    if (component.empty()) {
      continue;
    }

    // Normalizes a path joined from the buffer and the newly extracted path
    // component. This guarantees that a path starting with `..` cannot escape
    // out of root.
    const Try<std::string> normalizedPath = path::normalize(
        path::join(buffer.str(), component));
    if (normalizedPath.isError()) {
      return Error(normalizedPath.error());
    } else if (normalizedPath.get() == separator) {
      // Resets the buffer if the path gets normalized to root.
      buffer.str(separator);
      continue;
    }

    const Try<std::string> fullPath = path::normalize(
        path::join(root, normalizedPath.get()));
    if (fullPath.isError()) {
      return Error(fullPath.error());
    }

    struct ::stat s;
    if (::lstat(fullPath.get().c_str(), &s) == -1) {
      // Do not bail on non-existent paths.
      if (errno != ENOENT) {
        return ErrnoError("Failed to lstat '" + fullPath.get() + "'");
      }
    } else if (S_ISLNK(s.st_mode)) {
      const Try<std::string> linkPath = os::readlink(fullPath.get());
      if (linkPath.isError()) {
        return Error(
            "Failed to read symbolic link at '" + fullPath.get() + ": " +
            linkPath.error());
      }

      // Resets the buffer if the symlink is an absolute path.
      if (path::absolute(linkPath.get())) {
        buffer.str(separator);
      }

      // Prepends linkPath to the remaining unprocessedPath
      unprocessed = path::join(linkPath.get(), unprocessed);
      symlinks++;
      continue;
    }

    buffer << separator << component;
  }

  return path::normalize(buffer.str());
}

} // namespace os {

#endif // __STOUT_OS_POSIX_REALPATH_HPP__
