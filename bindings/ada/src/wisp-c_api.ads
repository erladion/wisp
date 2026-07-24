--  Thin, 1:1 Ada binding to the Wisp C client ABI (common/connectionapi.h).
--
--  Everything here mirrors the C header directly: NUL-terminated strings as
--  chars_ptr, raw buffers as System.Address plus length, int return codes.
--  Prefer the parent package Wisp for an idiomatic Ada API.

with Interfaces.C;         use Interfaces.C;
with Interfaces.C.Strings; use Interfaces.C.Strings;
with System;

package Wisp.C_API is

   --  Connection_Protocol
   PROTOCOL_ZMQ : constant := 0;

   --  Connection_Error_Code (returned by the functions below)
   SUCCESS                : constant := 0;
   ERROR_GENERIC          : constant := -1;
   ERROR_NO_CONNECTION    : constant := -2;
   ERROR_INVALID_ARGS     : constant := -3;
   ERROR_SEND_FAILED      : constant := -4;
   ERROR_BUFFER_TOO_SMALL : constant := -5;
   ERROR_TIMEOUT          : constant := -6;

   --  Wisp_Log_Level
   WISP_LOG_DEBUG   : constant := 0;
   WISP_LOG_INFO    : constant := 1;
   WISP_LOG_WARNING : constant := 2;
   WISP_LOG_ERROR   : constant := 3;

   type Connection_Config is record
      Address              : chars_ptr;  --  e.g. "tcp://127.0.0.1:5555"
      Client_Id            : chars_ptr;  --  Null_Ptr for a default name
      Protocol             : int;
      Keepalive_Time_Ms    : int;       --  heartbeat interval (default 3000)
      Keepalive_Timeout_Ms : int;       --  offline after this much silence (default 10000)
   end record
     with Convention => C;

   type Message_Callback is access procedure
     (Topic     : chars_ptr;
      Data      : System.Address;
      Len       : int;
      User_Data : System.Address)
     with Convention => C;

   type Log_Callback is access procedure
     (Level     : int;
      Message   : chars_ptr;
      User_Data : System.Address)
     with Convention => C;

   function Init_Connection
     (Config : access constant Connection_Config) return int
     with Import, Convention => C, External_Name => "initConnection";

   procedure Shutdown_Connection
     with Import, Convention => C, External_Name => "shutdownConnection";

   --  Message describing the most recent failure in a Wisp call on the
   --  calling thread, or "" if that call succeeded. Never null; valid until
   --  the next Wisp call on the same thread.
   function Last_Error_Message return chars_ptr
     with Import, Convention => C, External_Name => "lastErrorMessage";

   --  1 while the broker connection is up, 0 otherwise. Init_Connection
   --  returns before the connection finishes coming online.
   function Is_Connected return int
     with Import, Convention => C, External_Name => "isConnected";

   --  Blocks until the connection is up, for at most Timeout_Ms. Returns
   --  SUCCESS once connected and ERROR_TIMEOUT if not (the worker keeps
   --  retrying in the background); ERROR_NO_CONNECTION if Init_Connection
   --  was never called.
   function Wait_For_Connection (Timeout_Ms : int) return int
     with Import, Convention => C, External_Name => "waitForConnection";

   function Send_Data
     (Topic : chars_ptr; Data : System.Address; Len : int) return int
     with Import, Convention => C, External_Name => "sendData";

   function Send_Message (Topic : chars_ptr; Text : chars_ptr) return int
     with Import, Convention => C, External_Name => "sendMessage";

   --  Move the broker to a different discovery cluster at runtime. Name must be
   --  1-64 bytes without '|' (ERROR_INVALID_ARGS otherwise); the client must be
   --  connected (ERROR_NO_CONNECTION otherwise).
   function Set_Cluster (Name : chars_ptr) return int
     with Import, Convention => C, External_Name => "setCluster";

   function Reply_To_Sender (Data : System.Address; Len : int) return int
     with Import, Convention => C, External_Name => "replyToSender";

   --  Blocks the calling thread for up to Timeout_Ms waiting on the reply.
   --  On success fills Out_Buffer and Out_Len. Returns ERROR_NO_CONNECTION
   --  when offline, ERROR_TIMEOUT when no reply arrived in time, and
   --  ERROR_BUFFER_TOO_SMALL when the response did not fit (the reply is
   --  discarded; Out_Len is set to the required capacity).
   function Send_Request
     (Topic          : chars_ptr;
      Payload        : System.Address;
      Payload_Len    : int;
      Out_Buffer     : System.Address;
      Out_Buffer_Cap : int;
      Out_Len        : access int;
      Timeout_Ms     : int) return int
     with Import, Convention => C, External_Name => "sendRequest";

   --  User_Data is passed back to the callback and also identifies the
   --  registration for Unregister_Callback.
   procedure Register_Callback
     (Topic     : chars_ptr;
      Callback  : Message_Callback;
      User_Data : System.Address)
     with Import, Convention => C, External_Name => "registerCallback";

   --  Removes the registrations on Topic whose User_Data matches the value
   --  given to Register_Callback. A callback already being dispatched when
   --  this returns may still complete.
   procedure Unregister_Callback
     (Topic     : chars_ptr;
      User_Data : System.Address)
     with Import, Convention => C, External_Name => "unregisterCallback";

   --  Discard library log output below Level (a WISP_LOG_* value). The
   --  WISP_LOG_LEVEL environment variable sets the starting level; unset
   --  logs everything.
   procedure Set_Log_Level (Level : int)
     with Import, Convention => C, External_Name => "setLogLevel";

   --  Route library log output into Callback instead of stdout/stderr; null
   --  restores the default output. The callback runs on internal library
   --  threads; Message is only valid for the duration of the call.
   procedure Set_Log_Handler
     (Callback  : Log_Callback;
      User_Data : System.Address)
     with Import, Convention => C, External_Name => "setLogHandler";

end Wisp.C_API;
