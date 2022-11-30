unit istat.application;

{$mode delphi}{$H+}
interface

uses
  paxutils,
  istat.batch.decessi,
  istat.batch.popolazione,
  Classes, SysUtils, CustApp, istat.batch, ZDbcIntfs;

type
  { TIstat }
  TIstat = class(TCustomApplication)
  protected
    FProperties: TProperties;
    FReadListener: IItemReaderListener;
    FWriteListener: IItemWriterListener;
    function prepareConnection: IZConnection;
    function getConnection: IZConnection;
    function getConnectionDML: IZConnection;
    procedure prepareDatabase;
    procedure DoRun; override;
  public
    constructor Create(TheOwner: TComponent); override;
    destructor Destroy; override;
  end;

implementation

{ TIstat }

function TIstat.prepareConnection: IZConnection;
var
  info: TStringList;
  databaseLocation: string;
begin
  databaseLocation := FProperties.getProperty('database.path');
  info := TStringList.Create(True);
  info.AddPair('username', FProperties.getProperty('database.username'));
  info.AddPair('password', FProperties.getProperty('database.password'));
  info.AddPair('dialect', FProperties.getProperty('database.dialect'));
  info.AddPair('MaxConnections', FProperties.getProperty('database.MaxConnections'));
  info.AddPair('Wait', FProperties.getProperty('database.Wait'));
  info.AddPair('PageSize', FProperties.getProperty('database.PageSize'));
  info.AddPair('ForcedWrites', FProperties.getProperty('database.ForcedWrites'));
  info.AddPair('MaxUnflushedWrites', FProperties.getProperty('database.MaxUnflushedWrites'));
  if (not FileExists(databaseLocation)) then
    info.AddPair('CreateNewDatabase', 'CREATE DATABASE ''' + databaseLocation + ''' USER ''' + FProperties.getProperty('database.username') + ''' PASSWORD ''' + FProperties.getProperty('database.password') + ''' PAGE_SIZE ' + FProperties.getProperty('database.PageSize') + '');
  Result := DriverManager.GetConnectionWithParams('zdbc:firebird:/' + databaseLocation, info);
  Result.SetAutoCommit(False);
end;

function TIstat.getConnection: IZConnection;
begin
  Result := prepareConnection;
  Result.SetTransactionIsolation(tiRepeatableRead);
  Result.Open;
end;

function TIstat.getConnectionDML: IZConnection;
begin
  Result := prepareConnection;
  Result.Open;
end;

procedure TIstat.prepareDatabase;
var
  connection: IZConnection;
begin
  connection := getConnectionDML;
  try
    connection.CreateStatement.Execute('RECREATE TABLE ISTAT_DECESSI ' + LineEnding + // 0
      '( ' + LineEnding +// 0
      '    ITTER107 varchar(8), ' + LineEnding +// 0
      '    TERRITORIO varchar(100), ' + LineEnding +// 0
      '    TIPO_DATO15 varchar(50), ' + LineEnding +// 0
      '    ETA1 varchar(50), ' + LineEnding +// 0
      '    SEXISTAT1 varchar(2), ' + LineEnding +// 0
      '    STATCIV2 varchar(2), ' + LineEnding +// 0
      '    TITOLO_STUDIO varchar(50), ' + LineEnding +// 0
      '    T_BIS_A varchar(50), ' + LineEnding +// 0
      '    T_BIS_B varchar(50), ' + LineEnding +// 0
      '    ANNO_DI_NASCITA varchar(50), ' + LineEnding +// 0
      '    ETA1_B varchar(50), ' + LineEnding +// 0
      '    T_BIS_C varchar(50), ' + LineEnding +// 0
      '    ANNO_DI_MATRIMONIO varchar(50), ' + LineEnding +// 0
      '    ISO varchar(50), ' + LineEnding +// 0
      '    CAUSEMORTE_SL varchar(50), ' + LineEnding +// 0
      '    ANNO VARCHAR(20), ' + LineEnding +// 0
      '    VALORE integer, ' + LineEnding +// 0
      '    FLAG_CODES varchar(5) ' + LineEnding +// 0
      ')' + LineEnding +// 0
      '');
  except
    on E: Exception do
    begin
      WriteLn(e.Message);
    end;
  end;

  try
    Connection.CreateStatement.Execute('RECREATE TABLE ISTAT_POPOLAZIONE ' + LineEnding + // 0
      '( ' + LineEnding +// 0
      '    ITTER107 VARCHAR(50), ' + LineEnding + // 0
      '    Territorio VARCHAR(100), ' + LineEnding + // 0
      '    TIPO_DATO15 VARCHAR(50), ' + LineEnding + // 0
      '    Tipo_di_indicatore VARCHAR(50), ' + LineEnding + // 0
      '    SEXISTAT1 VARCHAR(50), ' + LineEnding + // 0
      '    Sesso VARCHAR(50), ' + LineEnding + // 0
      '    ETA1 VARCHAR(50), ' + LineEnding + // 0
      '    Eta VARCHAR(50), ' + LineEnding + // 0
      '    STATCIV2 VARCHAR(50), ' + LineEnding + // 0
      '    Stato_civile VARCHAR(50), ' + LineEnding + // 0
      '    ANNO VARCHAR(20), ' + LineEnding + // 0
      '    Seleziona_periodo VARCHAR(50), ' + LineEnding + // 0
      '    VALORE INTEGER, ' + LineEnding + // 0
      '    Flag_Codes VARCHAR(50), ' + LineEnding + // 0
      '    Flags VARCHAR(50) ' + LineEnding + // 0
      ')' + LineEnding + // 0
      '');
  except
    on E: Exception do
    begin
      WriteLn(e.Message);
    end;
  end;

  try
    connection.CreateStatement.Execute('CREATE INDEX ISTAT_DECESSI_KEY_1 ON ISTAT_DECESSI (CAUSEMORTE_SL);');
    connection.CreateStatement.Execute('CREATE INDEX ISTAT_DECESSI_KEY_2 ON ISTAT_DECESSI (ANNO);');
    connection.CreateStatement.Execute('CREATE INDEX ISTAT_DECESSI_KEY_3 ON ISTAT_DECESSI (TITOLO_STUDIO);');
    connection.CreateStatement.Execute('CREATE INDEX ISTAT_DECESSI_KEY_4 ON ISTAT_DECESSI (ISO);');
  except
    on E: Exception do
    begin
      WriteLn(e.Message);
    end;
  end;
  try
    Connection.CreateStatement.Execute('CREATE ASCENDING INDEX ISTAT_POPOLAZIONE_KEY_1 ON ISTAT_POPOLAZIONE (ITTER107)');
    Connection.CreateStatement.Execute('CREATE ASCENDING INDEX ISTAT_POPOLAZIONE_KEY_2 ON ISTAT_POPOLAZIONE (ETA1)');
    Connection.CreateStatement.Execute('CREATE ASCENDING INDEX ISTAT_POPOLAZIONE_KEY_3 ON ISTAT_POPOLAZIONE (SEXISTAT1)');
    Connection.CreateStatement.Execute('CREATE ASCENDING INDEX ISTAT_POPOLAZIONE_KEY_4 ON ISTAT_POPOLAZIONE (ANNO)');
    Connection.CreateStatement.Execute('CREATE ASCENDING INDEX ISTAT_POPOLAZIONE_KEY_5 ON ISTAT_POPOLAZIONE (STATCIV2)');
  except
    on E: Exception do
    begin
      WriteLn(e.Message);
    end;
  end;
  connection.Commit;
end;


procedure TIstat.DoRun;
var
  lastMillis: uint64;
  globalTime: uint64;
  activeTasks: TTaskQueue;

  task: TTask;
begin
  globalTime := millis();
  DefaultFormatSettings.ThousandSeparator := '.';
  DefaultFormatSettings.DecimalSeparator := ',';

  lastMillis := millis();
  activeTasks := TTaskQueue.Create;
  activeTasks.Semaphore := TSemaphore.Create(2);

  task := TTask.Create();
  task.runner := TPopolazioneRunner.Create(FProperties.getProperty('import.popolazione'), getConnection(), FReadListener, FWriteListener);
  activeTasks.add(task);

  task := TTask.Create();
  task.runner := TDecessiRunner.Create(FProperties.getProperty('import.decessi'), getConnection(), FReadListener, FWriteListener);
  activeTasks.add(task);

  prepareDatabase;

  activeTasks.Start;
  while activeTasks.workingCount > 0 do
  begin
    if millis() - lastMillis > 1000 then
    begin
      Writeln(Format('Active Threads: %d - Read: %10n -> Write: %10n in %s', [activeTasks.workingCount, FReadListener.Count * 1.0, FWriteListener.Count * 1.0, millisToString(millis() - lastMillis)], DefaultFormatSettings));
      lastMillis := millis();
    end;
  end;
  activeTasks.Stop;

  Writeln(Format('Total Read : %10n -> Write : %10n in %s', [FReadListener.totalCount * 1.0, FWriteListener.totalCount * 1.0, millisToString(millis() - globalTime)], DefaultFormatSettings));
  Terminate;
end;

constructor TIstat.Create(TheOwner: TComponent);
begin
  inherited Create(TheOwner);
  StopOnException := True;
  FReadListener := TItemReaderListener.Create;
  FWriteListener := TItemWriterListener.Create;
  FProperties := TProperties.Create;
  FProperties.load(ChangeFileExt(ApplicationName, '.properties'));
  if FileExists(FProperties.getProperty('database.path')) then
    DeleteFile(FProperties.getProperty('database.path'));
end;

destructor TIstat.Destroy;
begin
  FReadListener := nil;
  FWriteListener := nil;
  FreeAndNil(FProperties);
  inherited Destroy;
end;

end.
