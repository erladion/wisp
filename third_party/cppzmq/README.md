# cppzmq (vendored)

Header-only C++ binding for libzmq.

| | |
|---|---|
| Version | 4.11.0 (`CPPZMQ_VERSION_*` in `zmq.hpp`) |
| Upstream | https://github.com/zeromq/cppzmq |
| License | MIT (see the header of each file) |
| Files | `zmq.hpp`, `zmq_addon.hpp` — unmodified |

The build uses **these** headers, not any copy installed on the system, so
every machine compiles against the same cppzmq regardless of what the distro
ships (Debian/Ubuntu's `libzmq3-dev`, for example, carries 4.8.1). Configure
with `-DWISP_USE_SYSTEM_CPPZMQ=ON` to opt out and use the system headers
instead.

Only the C++ headers are vendored: **libzmq itself still comes from the
system** and is what the version above must stay compatible with.

To update: drop in the new `zmq.hpp` / `zmq_addon.hpp` from upstream and
change the version in this table.
