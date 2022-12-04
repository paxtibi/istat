unit istat.application;

{$mode delphi}{$H+}
interface

uses
  paxutils,
  istat.batch.decessi,
  istat.batch.popolazione,
  Classes, SysUtils, CustApp, istat.batch, ZDbcIntfs, ZDbcLogging;

type
  { TIstat }
  TIstat = class(TCustomApplication, IZLoggingListener)
  protected
    FProperties: TProperties;
    FReadListener: IItemReaderListener;
    FWriteListener: IItemWriterListener;
    FLoggingFormatter: TZLoggingFormatter;
    FLogFile: Text;
    function prepareConnection: IZConnection;
    function getConnection: IZConnection;
    function getConnectionDML: IZConnection;
    procedure prepareDatabase;
    procedure terminaAttivita;
    procedure DoRun; override;
  protected
    procedure LogEvent(Event: TZLoggingEvent);
  public
    constructor Create(TheOwner: TComponent); override;
    destructor Destroy; override;
  end;

implementation

uses
  LCLType;

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
    info.AddPair('CreateNewDatabase',
      'CREATE DATABASE ' + QuotedStr(databaseLocation) + // 0
      ' USER ' + QuotedStr(FProperties.getProperty('database.username')) + // 0
      ' PASSWORD ' + QuotedStr(FProperties.getProperty('database.password')) + // 0
      ' PAGE_SIZE ' + QuotedStr(FProperties.getProperty('database.PageSize')) + // 0
      ' DEFAULT CHARACTER SET ' + QuotedStr('UTF8') + // 0
      ' COLLATION ' + QuotedStr('UNICODE_CI_AI')
      ); // 0
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
      'ITTER107 VARCHAR(50), ' + LineEnding + //0
      'TERRITORIO VARCHAR(50), ' + LineEnding + //0
      'TIPO_DATO15 VARCHAR(50), ' + LineEnding + //0
      'TIPO_DATO VARCHAR(50), ' + LineEnding + //0
      'ETA1_A VARCHAR(50), ' + LineEnding + //0
      'ETA VARCHAR(50), ' + LineEnding + //0
      'SEXISTAT1 VARCHAR(50), ' + LineEnding + //0
      'SESSO VARCHAR(50), ' + LineEnding + //0
      'STATCIV2 VARCHAR(50), ' + LineEnding + //0
      'STATO_CIVILE VARCHAR(250), ' + LineEnding + //0
      'TITOLO_STUDIO VARCHAR(50), ' + LineEnding + //0
      'ISTRUZIONE VARCHAR(250), ' + LineEnding + //0
      'T_BIS_A VARCHAR(50), ' + LineEnding + //0
      'MESE_DI_DECESSO VARCHAR(50), ' + LineEnding + //0
      'T_BIS_B VARCHAR(50), ' + LineEnding + //0
      'ANNO_DI_NASCITA VARCHAR(50), ' + LineEnding + //0
      'ETA1_B VARCHAR(50), ' + LineEnding + //0
      'CLASSE_DI_ETA_CONIUGE VARCHAR(50), ' + LineEnding + //0
      'T_BIS_C VARCHAR(50), ' + LineEnding + //0
      'ANNO_DI_MATRIMONIO VARCHAR(50), ' + LineEnding + //0
      'ISO VARCHAR(50), ' + LineEnding + //0
      'PAESE_DI_CITTADINANZA VARCHAR(250), ' + LineEnding + //0
      'CAUSEMORTE_SL VARCHAR(50), ' + LineEnding + //0
      'CAUSA_INIZIALE_DI_MORTE VARCHAR(250), ' + LineEnding + //0
      'ANNO VARCHAR(50), ' + LineEnding + //0
      'SELEZIONA_PERIODO VARCHAR(50), ' + LineEnding + //0
      'VALORE VARCHAR(50), ' + LineEnding + //0
      'FLAG_CODES VARCHAR(50), ' + LineEnding + //0
      'FLAGS VARCHAR(50)' + LineEnding + //0
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
      '    TERRITORIO VARCHAR(100), ' + LineEnding + // 0
      '    TIPO_DATO15 VARCHAR(50), ' + LineEnding + // 0
      '    TIPO_DI_INDICATORE VARCHAR(50), ' + LineEnding + // 0
      '    SEXISTAT1 VARCHAR(50), ' + LineEnding + // 0
      '    SESSO VARCHAR(50), ' + LineEnding + // 0
      '    ETA1 VARCHAR(50), ' + LineEnding + // 0
      '    ETA VARCHAR(50), ' + LineEnding + // 0
      '    STATCIV2 VARCHAR(50), ' + LineEnding + // 0
      '    STATO_CIVILE VARCHAR(50), ' + LineEnding + // 0
      '    ANNO VARCHAR(20), ' + LineEnding + // 0
      '    SELEZIONA_PERIODO VARCHAR(50), ' + LineEnding + // 0
      '    VALORE INTEGER, ' + LineEnding + // 0
      '    FLAG_CODES VARCHAR(50), ' + LineEnding + // 0
      '    FLAGS VARCHAR(50) ' + LineEnding + // 0
      ')' + LineEnding + // 0
      '');
  except
    on E: Exception do
    begin
      WriteLn(e.Message);
    end;
  end;

  try
    Connection.CreateStatement.Execute('RECREATE TABLE ISTAT_POPOLAZIONE_ITALIANA ' + LineEnding + // 0
      '( ' + LineEnding +// 0
      '    ITTER107 VARCHAR(50), ' + LineEnding + // 0
      '    TERRITORIO VARCHAR(100), ' + LineEnding + // 0
      '    TIPO_DATO15 VARCHAR(50), ' + LineEnding + // 0
      '    TIPO_DI_INDICATORE VARCHAR(50), ' + LineEnding + // 0
      '    SEXISTAT1 VARCHAR(50), ' + LineEnding + // 0
      '    SESSO VARCHAR(50), ' + LineEnding + // 0
      '    ETA1 VARCHAR(50), ' + LineEnding + // 0
      '    ETA VARCHAR(50), ' + LineEnding + // 0
      '    STATCIV2 VARCHAR(50), ' + LineEnding + // 0
      '    STATO_CIVILE VARCHAR(50), ' + LineEnding + // 0
      '    ANNO VARCHAR(20), ' + LineEnding + // 0
      '    SELEZIONA_PERIODO VARCHAR(50), ' + LineEnding + // 0
      '    VALORE INTEGER, ' + LineEnding + // 0
      '    FLAG_CODES VARCHAR(50), ' + LineEnding + // 0
      '    FLAGS VARCHAR(50) ' + LineEnding + // 0
      ')' + LineEnding + // 0
      '');
  except
    on E: Exception do
    begin
      WriteLn(e.Message);
    end;
  end;
  connection.Commit;
end;

procedure TIstat.terminaAttivita;
var
  connection: IZConnection;
begin
  connection := getConnectionDML;
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

  Connection.CreateStatement.Execute('INSERT INTO ISTAT_POPOLAZIONE_ITALIANA SELECT * FROM ISTAT_POPOLAZIONE WHERE ITTER107=''IT''');
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

  prepareDatabase;

  task := TTask.Create();
  task.runner := TPopolazioneRunner.Create(FProperties.getProperty('import.popolazione'), getConnection(), FReadListener, FWriteListener);
  activeTasks.add(task);

  task := TTask.Create();
  task.runner := TDecessiRunner.Create(FProperties.getProperty('import.decessi'), getConnection(), FReadListener, FWriteListener);
  activeTasks.add(task);

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
  activeTasks.Terminate;

  Writeln(Format('Total Read : %10n -> Write : %10n in %s', [FReadListener.totalCount * 1.0, FWriteListener.totalCount * 1.0, millisToString(millis() - globalTime)], DefaultFormatSettings));
  terminaAttivita;
  Terminate;
end;

procedure TIstat.LogEvent(Event: TZLoggingEvent);
begin
  if Event.Category in [lcExecPrepStmt, lcBindPrepStmt] then exit;
  WriteLn(FLogFile, FLoggingFormatter.Format(Event));
  Flush(FLogFile);
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
  if FileExists(ChangeFileExt(ParamStr(0), '.log')) then
    DeleteFile(ChangeFileExt(ParamStr(0), '.log'));
  AssignFile(FLogFile, ChangeFileExt(ParamStr(0), '.log'));
  Rewrite(FLogFile);

  FLoggingFormatter := TZLoggingFormatter.Create;
  DriverManager.AddLoggingListener(self);
end;

destructor TIstat.Destroy;
begin
  Flush(FLogFile);
  FReadListener := nil;
  FWriteListener := nil;
  FreeAndNil(FProperties);
  DriverManager.RemoveLoggingListener(Self);
  inherited Destroy;
end;

end.
