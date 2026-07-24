# Builds the Ada binding demo against the CMake build tree.
#
#   make [BUILD=/path/to/cmake/build]
#
# Requires gnat + gprbuild and a completed CMake build (libwispcore.a).
# pkg-config resolves the static protobuf/abseil closure; point
# PKG_CONFIG_PATH elsewhere if your protobuf is not under ~/.local.

BUILD ?= $(abspath ../../build)
PKG_CONFIG_PATH ?= $(HOME)/.local/lib/pkgconfig

PROTO_LIBS := $(shell PKG_CONFIG_PATH=$(PKG_CONFIG_PATH) pkg-config --libs --static protobuf)
LINK_LIBS  := -L$(BUILD)/common -lwispcore -lzmq $(PROTO_LIBS) -lstdc++ -lrt -lm

all:
	gprbuild -P wisp_ada.gpr -largs $(LINK_LIBS)

clean:
	rm -rf obj bin

.PHONY: all clean
