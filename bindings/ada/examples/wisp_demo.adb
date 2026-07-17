--  Demo for the Ada binding, in two roles:
--
--    bin/wisp_demo listen    subscribe to demo.chat and answer demo.echo
--    bin/wisp_demo send      publish on demo.chat, then request demo.echo
--
--  Start a broker (./build/server/server) and a listener first, then send.
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
begin
   if Role = "listen" then
      Wisp.Connect (Address => Broker, Client_Id => "ada-listener");
      Wisp.Subscribe ("demo.chat", Demo_Handlers.Print'Access);
      Wisp.Subscribe ("demo.echo", Demo_Handlers.Echo'Access);
      Put_Line ("listening on demo.chat / demo.echo (Ctrl-C to stop)");
      loop
         delay 3600.0;
      end loop;
   else
      Wisp.Connect (Address => Broker, Client_Id => "ada-sender");

      Wisp.Send ("demo.chat", "hello from Ada");
      Put_Line
        ("request answered: "
         & Wisp.Request ("demo.echo", "ping", Timeout_Ms => 2_000));

      Wisp.Shutdown;
   end if;
end Wisp_Demo;
