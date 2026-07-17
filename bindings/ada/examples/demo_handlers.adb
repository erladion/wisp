with Ada.Text_IO; use Ada.Text_IO;

with Wisp;

package body Demo_Handlers is

   procedure Print (Topic : String; Data : String) is
   begin
      Put_Line ("[" & Topic & "] " & Data);
   end Print;

   procedure Echo (Topic : String; Data : String) is
      pragma Unreferenced (Topic);
   begin
      Wisp.Reply ("echo: " & Data);
   end Echo;

end Demo_Handlers;
