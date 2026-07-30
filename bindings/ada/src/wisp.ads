--  Idiomatic Ada binding for the Wisp message broker client.
--
--  A thick wrapper over the C ABI in common/connectionapi.h: Ada strings,
--  exceptions instead of error codes, and plain Ada procedures as
--  subscription callbacks. Subprogram names mirror the C ABI's, so the
--  header documentation applies 1:1; the thin mapping lives in Wisp.C_API.
--
--  The connection is process-global, like the underlying C API:
--  Init_Connection once, use freely from any task, Shutdown_Connection at
--  exit. Payload Strings are treated as raw bytes (binary-safe, no encoding
--  assumed).

package Wisp is

   Wisp_Error : exception;
   --  Raised when the underlying library reports failure. The exception
   --  message names the operation and the C error code.

   procedure Init_Connection
     (Address              : String;            --  e.g. "tcp://127.0.0.1:5555"
      Client_Id            : String   := "";    --  "" lets the library choose
      Keepalive_Time_Ms    : Positive := 3_000;   --  heartbeat interval; keep below the broker's 10 s zombie timeout
      Keepalive_Timeout_Ms : Positive := 10_000); --  offline after this much broker silence
   --  Open the connection. Returns before it finishes coming online; use
   --  Wait_For_Connection to block for it (see also Is_Connected).

   procedure Wait_For_Connection (Timeout_Ms : Positive := 5_000);
   --  Block until the connection is up, raising Wisp_Error if the broker
   --  cannot be reached within Timeout_Ms. A timeout is not terminal - the
   --  connection keeps being retried in the background.

   procedure Shutdown_Connection;

   function Is_Connected return Boolean;
   --  True while the broker connection is up.

   procedure Send_Data (Topic : String; Data : String);
   --  Publish Data on Topic (fire and forget).

   procedure Send_Message (Topic : String; Text : String);
   --  Publish Text on Topic (fire and forget). Convenience over Send_Data
   --  for NUL-free text payloads.

   procedure Set_Cluster (Name : String);

   --  Publish on Topic, naming Reply_Topic for a responder to answer on - the
   --  non-blocking half of request/reply. Register a handler on Reply_Topic
   --  first, send, and handle the answer there. Send_Request below does the
   --  same and then blocks, which a message handler must not do: it would
   --  stall the thread delivering the reply.
   --
   --  Nothing expires here; unregister the reply topic when you stop waiting.
   --  Raises Wisp_Error if Reply_Topic is empty, over 512 bytes, or starts with
   --  "__" - a broker drops reserved keys rather than routing them, so such an
   --  answer would be lost in silence.
   procedure Send_Data_With_Reply
     (Topic : String; Data : String; Reply_Topic : String);

   --  A reply topic unique to this request, derived from Request_Topic and
   --  within the broker's 512-byte topic limit.
   function Make_Reply_Topic (Request_Topic : String) return String;
   --  Move the broker to a different discovery cluster at runtime. Name must be
   --  1-64 bytes without '|'; raises Wisp_Error if it is rejected or there is no
   --  connection. Any connected client may do this — the broker re-targets its
   --  beacons and re-meshes. No effect on a broker started without discovery.

   procedure Reply_To_Sender (Data : String);
   --  Reply to the sender of the message currently being handled; only
   --  meaningful from inside a subscription handler.

   function Send_Request
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

   procedure Register_Callback (Topic : String; Callback : not null Handler);
   --  Register Callback for Topic.

   procedure Unregister_Callback (Topic : String; Callback : not null Handler);
   --  Remove a registration made with Register_Callback. A handler already
   --  running when this returns may still complete its current message.

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
