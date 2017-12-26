#!/bin/sh
set -e

usage() {
  this=$1
  cat <<EOF
$this: download go binaries for streamsets/datacollector-edge

Usage: $this [version]
   [version] is a version number from https://streamsets.com/opensource/
   If version is missing, then latest build from master is downloaded.

EOF
  exit 2
}

parse_args() {
  #BINDIR is /usr/local/bin unless set be ENV
  # over-ridden by flag below

  BINDIR=${BINDIR:-/usr/local/bin}
  while getopts "b:h?" arg; do
    case "$arg" in
      b) BINDIR="$OPTARG" ;;
      h | \?) usage "$0" ;;
    esac
  done
  shift $((OPTIND - 1))
  VERSION=$1
}
# this function wraps all the destructive operations
# if a curl|bash cuts off the end of the script due to
# network, either nothing will happen or will syntax error
# out preventing half-done work
execute() {
  TMPDIR=.
  echo "$PREFIX: downloading ${TARBALL_URL}"
  http_download "${TMPDIR}/${TARBALL}" "${TARBALL_URL}"

  (cd "${TMPDIR}" && untar "${TARBALL}")
  echo "$PREFIX: extracted in ${TMPDIR}"
  echo "Running SDC Edge: "
  echo "${TMPDIR}/streamsets-datacollector-edge/bin/edge -logToConsole"
}
is_supported_platform() {
  platform=$1
  found=1
  case "$platform" in
    darwin/amd64) found=0 ;;
    darwin/386) found=0 ;;
    linux/amd64) found=0 ;;
    linux/386) found=0 ;;
    windows/amd64) found=0 ;;
    windows/386) found=0 ;;
    freebsd/amd64) found=0 ;;
    freebsd/386) found=0 ;;
    netbsd/amd64) found=0 ;;
    netbsd/386) found=0 ;;
    openbsd/amd64) found=0 ;;
    openbsd/386) found=0 ;;
  esac
  return $found
}
check_platform() {
  if is_supported_platform "$PLATFORM"; then
    # optional logging goes here
    true
  else
    echo "${PREFIX}: platform $PLATFORM is not supported.  Make sure this script is up-to-date and file request at https://issues.streamsets.com"
    exit 1
  fi
}
adjust_tarball_url() {
  if [ -z "${VERSION}" ]; then
    VERSION=3.0.0.0
  fi
  TARBALL=streamsets-datacollector-edge-${VERSION}-${OS}-${ARCH}.tgz
  TARBALL_URL=https://archives.streamsets.com/datacollector/${VERSION}/tarball/SDCe/${TARBALL}
}
adjust_format() {
  # change format (tar.gz or zip) based on ARCH
  true
}
adjust_os() {
  # adjust archive name based on OS
  true
}
adjust_arch() {
  # adjust archive name based on ARCH
  true
}

cat /dev/null <<EOF
------------------------------------------------------------------------
https://github.com/client9/shlib - portable posix shell functions
Public domain - http://unlicense.org
https://github.com/client9/shlib/blob/master/LICENSE.md
but credit (and pull requests) appreciated.
------------------------------------------------------------------------
EOF
is_command() {
  command -v "$1" >/dev/null
}
uname_os() {
  os=$(uname -s | tr '[:upper:]' '[:lower:]')
  echo "$os"
}
uname_arch() {
  arch=$(uname -m)
  case $arch in
    x86_64) arch="amd64" ;;
    x86) arch="386" ;;
    i686) arch="386" ;;
    i386) arch="386" ;;
    aarch64) arch="arm64" ;;
    armv5*) arch="arm5" ;;
    armv6*) arch="arm6" ;;
    armv7*) arch="arm7" ;;
  esac
  echo ${arch}
}
uname_os_check() {
  os=$(uname_os)
  case "$os" in
    darwin) return 0 ;;
    dragonfly) return 0 ;;
    freebsd) return 0 ;;
    linux) return 0 ;;
    android) return 0 ;;
    nacl) return 0 ;;
    netbsd) return 0 ;;
    openbsd) return 0 ;;
    plan9) return 0 ;;
    solaris) return 0 ;;
    windows) return 0 ;;
  esac
  echo "$0: uname_os_check: internal error '$(uname -s)' got converted to '$os' which is not a GOOS value. Please file bug at https://github.com/client9/shlib"
  return 1
}
uname_arch_check() {
  arch=$(uname_arch)
  case "$arch" in
    386) return 0 ;;
    amd64) return 0 ;;
    arm64) return 0 ;;
    armv5) return 0 ;;
    armv6) return 0 ;;
    armv7) return 0 ;;
    ppc64) return 0 ;;
    ppc64le) return 0 ;;
    mips) return 0 ;;
    mipsle) return 0 ;;
    mips64) return 0 ;;
    mips64le) return 0 ;;
    s390x) return 0 ;;
    amd64p32) return 0 ;;
  esac
  echo "$0: uname_arch_check: internal error '$(uname -m)' got converted to '$arch' which is not a GOARCH value.  Please file bug report at https://github.com/client9/shlib"
  return 1
}
untar() {
  tarball=$1
  case "${tarball}" in
    *.tar.gz | *.tgz) tar -xzf "${tarball}" ;;
    *.tar) tar -xf "${tarball}" ;;
    *.zip) unzip "${tarball}" ;;
    *)
      echo "Unknown archive format for ${tarball}"
      return 1
      ;;
  esac
}
mktmpdir() {
  test -z "$TMPDIR" && TMPDIR="$(mktemp -d)"
  mkdir -p "${TMPDIR}"
  echo "${TMPDIR}"
}
http_download() {
  local_file=$1
  source_url=$2
  header=$3
  headerflag=''
  destflag=''
  if is_command curl; then
    cmd='curl --fail -sSL'
    destflag='-o'
    headerflag='-H'
  elif is_command wget; then
    cmd='wget -q'
    destflag='-O'
    headerflag='--header'
  else
    echo "http_download: unable to find wget or curl"
    return 1
  fi
  if [ -z "$header" ]; then
    $cmd $destflag "$local_file" "$source_url"
  else
    $cmd $headerflag "$header" $destflag "$local_file" "$source_url"
  fi
}
cat /dev/null <<EOF
------------------------------------------------------------------------
End of functions from https://github.com/client9/shlib
------------------------------------------------------------------------
EOF

OWNER=streamsets
REPO=datacollector-edge
FORMAT=tar.gz
OS=$(uname_os)
ARCH=$(uname_arch)
PREFIX="$OWNER/$REPO"
PLATFORM="${OS}/${ARCH}"

uname_os_check "$OS"

uname_arch_check "$ARCH"

parse_args "$@"

check_platform

adjust_format

adjust_os

adjust_arch

adjust_tarball_url

echo "$PREFIX: found version ${VERSION} for ${OS}/${ARCH}"

execute
