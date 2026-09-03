--  Demo for the Ada binding, in two roles:
--
--    bin/wisp_demo listen    subscribe to demo.chat and demo.reading, answer demo.echo
--    bin/wisp_demo send      publish on demo.chat and demo.reading, then request demo.echo
--
--  Start a broker (./build/server/wisp-broker) and a listener first, then send.
--  Two processes are required: the broker never routes a message back to
--  its sender.

with Ada.Command_Line; use Ada.Command_Line;
with Ada.Text_IO;      use Ada.Text_IO;

with Demo_Handlers;
with Wisp;

procedure Wisp_Demo is
   Broker : constant String := "tcp://127.0.0.1:5555";
   Role   : constant String :=
     (if Argument_Count > 0 then Argument (1) else "send");

   --  One serialized `demo.Reading { string sensor = 1; }`, hand-encoded so the
   --  demo needs no protobuf library: a length-delimited field is its tag byte,
   --  its length as a varint, then the bytes (one byte of length is enough
   --  below 128). A real application produces this from its .proto instead -
   --  protobuf-c bound through Interfaces.C is the usual route - and passes the
   --  result to Send_Any exactly as this does.
   --
   --  Nothing in Wisp knows what a demo.Reading is, and nothing needs to: the
   --  broker forwards the payload untouched and the envelope only carries the
   --  name so the receiver can check it.
   function Reading (Sensor : String) return String is
     (Character'Val (16#0A#) & Character'Val (Sensor'Length) & Sensor);
begin
   if Role = "listen" then
      Wisp.Init_Connection (Address => Broker, Client_Id => "ada-listener");
      Wisp.Wait_For_Connection;
      Wisp.Register_Callback ("demo.chat", Demo_Handlers.Print'Access);
      Wisp.Register_Callback ("demo.echo", Demo_Handlers.Echo'Access);
      Wisp.Register_Any_Callback
        ("demo.reading", Demo_Handlers.Print_Any'Access);
      Put_Line
        ("listening on demo.chat / demo.reading / demo.echo (Ctrl-C to stop)");
      loop
         delay 3600.0;
      end loop;
   else
      Wisp.Init_Connection (Address => Broker, Client_Id => "ada-sender");
      Wisp.Wait_For_Connection;

      Wisp.Send_Message ("demo.chat", "hello from Ada");
      Wisp.Send_Any ("demo.reading", "demo.Reading", Reading ("ada-sensor-1"));
      Put_Line
        ("request answered: "
         & Wisp.Send_Request ("demo.echo", "ping", Timeout_Ms => 2_000));

      Wisp.Shutdown_Connection;
   end if;
end Wisp_Demo;
