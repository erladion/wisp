# Header-only adapter that turns Wisp's callbacks into a per-frame drain, for
# immediate-mode UIs and game loops (Dear ImGui, SDL, raylib, ...). It pulls in
# no UI framework of its own, so unlike the Qt binding it is always available.
add_library(WispPolling INTERFACE)
target_include_directories(WispPolling INTERFACE ${CMAKE_CURRENT_SOURCE_DIR})
target_link_libraries(WispPolling INTERFACE WispCore)
