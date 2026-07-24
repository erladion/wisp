--  Idiomatic Ada binding for the Wisp message broker client.
--
--  A thick wrapper over the C ABI in common/connectionapi.h: Ada strings,
--  exceptions instead of error codes, and plain Ada procedures as
--  subscription callbacks. The thin 1:1 mapping lives in Wisp.C_API.
--
--  The connection is process-global, like the underlying C API: Connect
--  once, use freely from any task, Shutdown at exit. Payload Strings are
--  treated as raw bytes (binary-safe, no encoding assumed).

package Wisp is

   Wisp_Error : exception;
   --  Raised when the underlying library reports failure. The exception
   --  message names the operation and the C error code.

   procedure Connect
     (Address              : String;            --  e.g. "tcp://127.0.0.1:5555"
      Client_Id            : String   := "";    --  "" lets the library choose
      Keepalive_Time_Ms    : Positive := 3_000;   --  heartbeat interval; keep below the broker's 10 s zombie timeout
      Keepalive_Timeout_Ms : Positive := 10_000;  --  offline after this much broker silence
      Connect_Timeout_Ms   : Natural  := 5_000);
   --  Connect and block until the connection is up, raising Wisp_Error if
   --  the broker cannot be reached within Connect_Timeout_Ms. Zero skips
   --  the wait (the connection then comes up in the background; see
   --  Is_Connected).

   procedure Shutdown;

   function Is_Connected return Boolean;
   --  True while the broker connection is up.

   procedure Send (Topic : String; Data : String);
   --  Publish Data on Topic (fire and forget).

   procedure Set_Cluster (Name : String);
   --  Move the broker to a different discovery cluster at runtime. Name must be
   --  1-64 bytes without '|'; raises Wisp_Error if it is rejected or there is no
   --  connection. Any connected client may do this — the broker re-targets its
   --  beacons and re-meshes. No effect on a broker started without discovery.

   procedure Reply (Data : String);
   --  Reply to the sender of the message currently being handled; only
   --  meaningful from inside a subscription handler.

   function Request
     (Topic        : String;
      Payload      : String;
      Timeout_Ms   : Positive := 5_000;
      Max_Response : Positive := 65_536) return String;
   --  Send Payload on Topic and block for the reply. Raises Wisp_Error on
   --  timeout, when offline, or if the response does not fit in
   --  Max_Response bytes (the message then names the required size).

   type Handler is access procedure (Topic : String; Data : String);
   --  Must designate a library-level procedure. Handlers run on the
   --  library's worker thread, not on any Ada task: keep them short and
   --  synchronize access to shared state. Exceptions raised inside a
   --  handler are discarded (they must not propagate into C).

   procedure Subscribe (Topic : String; Callback : not null Handler);
   --  Register Callback for Topic.

   procedure Unsubscribe (Topic : String; Callback : not null Handler);
   --  Remove a registration made with Subscribe. A handler already running
   --  when this returns may still complete its current message.

   type Log_Level is (Debug, Info, Warning, Error);

   procedure Set_Log_Level (Level : Log_Level);
   --  Discard library log output below Level. The WISP_LOG_LEVEL environment
   --  variable ("debug", "info", "warn", "error") sets the starting level;
   --  unset logs everything.

   type Log_Handler is access procedure (Level : Log_Level; Message : String);
   --  Must designate a library-level procedure. Like Handler, it runs on the
   --  library's worker threads: keep it short, synchronize access to shared
   --  state, and do not call back into Wisp from it. Exceptions raised
   --  inside are discarded.

   procedure Set_Log_Handler (Callback : Log_Handler);
   --  Route library log output into Callback instead of stdout/stderr; null
   --  restores the default output. The Set_Log_Level filter applies either
   --  way.

end Wisp;
