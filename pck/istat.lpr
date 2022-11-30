program istat;

{$mode objfpc}{$H+}

uses
 {$IFDEF UNIX}
  cthreads,
         {$ENDIF}
  Classes,
  SysUtils,
  CustApp,
  istat.application,
  zcore,
  zdbc,
  zparsesql,
  zplain, istat.batch, istat.batch.decessi, istat.batch.popolazione;

var
  Application: TIstat;
begin
  Application := TIstat.Create(nil);
  Application.Title := 'Istat';
  Application.Run;
  Application.Free;
end.
