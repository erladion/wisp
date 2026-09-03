with Ada.Text_IO; use Ada.Text_IO;

with Wisp;

package body Demo_Handlers is

   procedure Print (Topic : String; Data : String) is
   begin
      Put_Line ("[" & Topic & "] " & Data);
   end Print;

   procedure Print_Any (Topic, Type_Name, Value : String) is
   begin
      Put_Line
        ("[" & Topic & "] packed " & Type_Name & " ("
         & Integer'Image (Value'Length) & " bytes)");
   end Print_Any;

   procedure Echo (Topic : String; Data : String) is
      pragma Unreferenced (Topic);
   begin
      Wisp.Reply_To_Sender ("echo: " & Data);
   end Echo;

end Demo_Handlers;
