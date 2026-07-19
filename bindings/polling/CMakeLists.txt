# Header-only adapter that turns Wisp's callbacks into a per-frame drain, for
# immediate-mode UIs and game loops (Dear ImGui, SDL, raylib, ...). It pulls in
# no UI framework of its own, so unlike the Qt binding it is always available.
add_library(WispPolling INTERFACE)
target_include_directories(WispPolling INTERFACE
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>
    $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>
    $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}/wisp>
)
target_link_libraries(WispPolling INTERFACE WispCore)
set_target_properties(WispPolling PROPERTIES EXPORT_NAME Polling)
add_library(Wisp::Polling ALIAS WispPolling)

install(TARGETS WispPolling EXPORT WispTargets)
install(FILES wisppoller.h DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}/wisp)
