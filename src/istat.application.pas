unit istat.application;

{$mode delphi}{$H+}
{$ModeSwitch typehelpers}
{$ModeSwitch advancedrecords}
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
    procedure CreateIndex(indexName, onTable, columns: string);
    procedure prepareDatabase;
    procedure terminaAttivita;
    procedure DoRun; override;
  protected
    procedure LogEvent(Event: TZLoggingEvent);
  public
    constructor Create(TheOwner: TComponent); override;
    destructor Destroy; override;
  end;

type
  TTimer = uint64;

  { TTimerHelper }

  TTimerHelper = type helper for TTimer
    procedure restart;
    function elapsed: TTimer;
    function toString: string;
  end;

implementation

uses
  LCLType;


{ TTimerHelper }

procedure TTimerHelper.restart;
begin
  self := millis();
end;

function TTimerHelper.elapsed: TTimer;
begin
  Result := millis() - Self;
end;

function TTimerHelper.toString: string;
begin
  Result := millisToString(elapsed);
end;


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

procedure TIstat.CreateIndex(indexName, onTable, columns: string);
var
  connection: IZConnection;
  timer: TTimer = 0;
begin
  connection := getConnectionDML;
  try
    timer.restart;
    WriteLn('  INDEX ', indexName);
    connection.CreateStatement.Execute(Format('CREATE INDEX %s ON %s (%s)', [indexName, onTable, columns]));
    connection.Commit;
    WriteLn('  INDEX ', indexName, ' in ', timer.toString);
  except
    on e: Exception do
    begin
      Writeln(e.Message);
    end;
  end;
end;

procedure TIstat.prepareDatabase;
var
  connection: IZConnection;
begin
  connection := getConnectionDML;
  WriteLn('Creazione tabelle');
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
    connection.CreateStatement.Execute('RECREATE TABLE ISTAT_DECESSI_ITALIA ' + LineEnding + // 0
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
  timer: TTimer = 0;
begin
  connection := getConnectionDML;
  timer.restart;
  WriteLn('Creazione indici');
  CreateIndex('ISTAT_DECESSI_KEY_01', 'ISTAT_DECESSI', 'ITTER107');
  CreateIndex('ISTAT_DECESSI_KEY_02', 'ISTAT_DECESSI', 'TIPO_DATO15');
  CreateIndex('ISTAT_DECESSI_KEY_03', 'ISTAT_DECESSI', 'ETA1_A');
  CreateIndex('ISTAT_DECESSI_KEY_04', 'ISTAT_DECESSI', 'SEXISTAT1');
  CreateIndex('ISTAT_DECESSI_KEY_05', 'ISTAT_DECESSI', 'SEXISTAT1');
  CreateIndex('ISTAT_DECESSI_KEY_06', 'ISTAT_DECESSI', 'TITOLO_STUDIO');
  CreateIndex('ISTAT_DECESSI_KEY_07', 'ISTAT_DECESSI', 'T_BIS_A');
  CreateIndex('ISTAT_DECESSI_KEY_08', 'ISTAT_DECESSI', 'T_BIS_B');
  CreateIndex('ISTAT_DECESSI_KEY_09', 'ISTAT_DECESSI', 'ETA1_B');
  CreateIndex('ISTAT_DECESSI_KEY_10', 'ISTAT_DECESSI', 'ISO');
  CreateIndex('ISTAT_DECESSI_KEY_11', 'ISTAT_DECESSI', 'CAUSEMORTE_SL');
  CreateIndex('ISTAT_DECESSI_KEY_12', 'ISTAT_DECESSI', 'ANNO');
  CreateIndex('ISTAT_POPOLAZIONE_KEY_01', 'ISTAT_POPOLAZIONE', 'ITTER107');
  CreateIndex('ISTAT_POPOLAZIONE_KEY_02', 'ISTAT_POPOLAZIONE', 'ETA1');
  CreateIndex('ISTAT_POPOLAZIONE_KEY_03', 'ISTAT_POPOLAZIONE', 'SEXISTAT1');
  CreateIndex('ISTAT_POPOLAZIONE_KEY_04', 'ISTAT_POPOLAZIONE', 'ANNO');
  CreateIndex('ISTAT_POPOLAZIONE_KEY_05', 'ISTAT_POPOLAZIONE', 'STATCIV2');
  WriteLn('Creazione indici in ', timer.toString);
  WriteLn('Travaso dati in "ISTAT_POPOLAZIONE_ITALIANA"');
  timer.restart;
  Connection.CreateStatement.Execute('INSERT INTO ISTAT_POPOLAZIONE_ITALIANA SELECT * FROM ISTAT_POPOLAZIONE WHERE ITTER107=''IT'' and SEXISTAT1=9 and STATCIV2=99');
  Connection.Commit;
  WriteLn('Tabella "POPOLAZIONE ITALIANA" travasata in ', timer.ToString);
  WriteLn('Travaso dati in "ISTAT_DECESSI_ITALIANA"');
  timer.restart;
  Connection.CreateStatement.Execute('INSERT INTO ISTAT_DECESSI_ITALIANA SELECT * FROM ISTAT_POPOLAZIONE WHERE ITTER107=''IT'' and R.SEXISTAT1 = 9 and R.STATCIV2 = 99 and R.TITOLO_STUDIO = 99 and R.ISO = ''IT''');
  Connection.Commit;
  WriteLn('Tabella "ISTAT_DECESSI_ITALIANA ITALIANA" travasata in ', timer.ToString);
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
    if millis() - lastMillis > 1000 * 60 then
    begin
      Writeln(Format('Active Threads: %d - Read: %15n -> Write: %15n in %s', [activeTasks.workingCount, FReadListener.Count * 1.0, FWriteListener.Count * 1.0, millisToString(millis() - lastMillis)], DefaultFormatSettings));
      lastMillis := millis();
    end;
  end;
  activeTasks.Stop;
  activeTasks.Terminate;

  Writeln(Format('Total Read : %15n -> Write : %15n in %s', [FReadListener.totalCount * 1.0, FWriteListener.totalCount * 1.0, millisToString(millis() - globalTime)], DefaultFormatSettings));
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
