unit istat.batch.firebird;

{$mode delphi}{$H+}

interface

uses
  Classes, SysUtils, paxutils, ZDbcIntfs, istat.batch;

type
  { TFirebirdChunkedDatabaseWriter }
  TFirebirdChunkedDatabaseWriter<TItemType> = class(TAbstractChunkedDatabaseWriter<TItemType>)
  protected
    function statement(item: TItemType): rawbytestring; virtual; abstract;
    function getTableName: string; virtual; abstract;
    function ExecuteBlock(Statements: string; Count: integer): IRunnable;
  public
    function Open: boolean; override;
    function Close: boolean; override;
    procedure Write(const items: IChunk<TItemType>); override;
    property TableName: string read getTableName;
  end;

  { TFirebirdDatabaseWriter }
  TFirebirdDatabaseWriter<TItemType> = class(TAbstractDatabaseWriter<TItemType>)
  private
    FTableName: string;
    procedure SetTableName(AValue: string);
  protected
    FStatement: IZPreparedStatement;
    function GetTableName: string; virtual; abstract;
  public
    function Open: boolean; override;
    function Close: boolean; override;
    property TableName: string read FTableName write SetTableName;
  end;

  { TFirebirdStepRunner }
  TFirebirdStepRunner<TItemType> = class(TStepRunner<TItemType>, IRunnable)
  public
    constructor Create(aFileName: TFileName; aConnection: IZConnection; aReaderListener: IItemReaderListener; aWriterListener: IItemWriterListener);
  end;


implementation

{ TFirebirdStepRunner }

constructor TFirebirdStepRunner<TItemType>.Create(aFileName: TFileName; aConnection: IZConnection; aReaderListener: IItemReaderListener; aWriterListener: IItemWriterListener);
begin
  inherited Create(aFileName, aConnection, aReaderListener, aWriterListener);
end;

{ TFirebirdChunkedDatabaseWriter }

function TFirebirdChunkedDatabaseWriter<TItemType>.ExecuteBlock(Statements: string; Count: integer): IRunnable;
var
  sql: string;
  runner: TExecuteStatementRunner;
begin
  runner := TExecuteStatementRunner.Create;
  runner.TarghetConnection := FConnection;
  sql := 'execute block' + LineEnding + // 0
    'as' + LineEnding +// 0
    '  begin' + LineEnding +//0
    statements + LineEnding +//0
    'end;';
  runner.Statement := sql;
  runner.Count := Count;
  runner.WriterListerner := FListener;
  Result := runner;
end;

function TFirebirdChunkedDatabaseWriter<TItemType>.Open: boolean;
var
  RS: IZResultSet;
begin
  Result := inherited Open;
  RS := FConnection.CreateStatement.ExecuteQuery('SELECT r.RDB$INDEX_NAME FROM RDB$INDICES r WHERE r.RDB$INDEX_INACTIVE = 0 and RDB$RELATION_NAME = ' + QuotedStr(TableName));
  while RS.Next do
  begin
    FConnection.CreateStatement.Execute('ALTER INDEX ' + RS.GetAnsiString(1) + ' INACTIVE');
  end;
  FConnection.Commit;
end;

function TFirebirdChunkedDatabaseWriter<TItemType>.Close: boolean;
var
  RS: IZResultSet;
begin
  Result := inherited Close;
  RS := FConnection.CreateStatement.ExecuteQuery('SELECT r.RDB$INDEX_NAME FROM RDB$INDICES r WHERE r.RDB$INDEX_INACTIVE = 0 and RDB$RELATION_NAME = ' + QuotedStr(TableName));
  while RS.Next do
  begin
    FConnection.CreateStatement.Execute('ALTER INDEX ' + RS.GetAnsiString(1) + ' ACTIVE');
  end;
  FConnection.Commit;
end;

procedure TFirebirdChunkedDatabaseWriter<TItemType>.Write(const items: IChunk<TItemType>);
var
  sql: string;
  idx: integer;
  Count: integer;
  task: TTask;
  p: TItemType;
const
  activeTasks: TTaskQueue = nil;
begin
  if activeTasks = nil then
  begin
    activeTasks := TTaskQueue.Create;
    activeTasks.Semaphore := TSemaphore.Create(100);
  end;
  Count := 0;
  sql := '';
  activeTasks.Start;
  for idx := 0 to items.Count - 1 do
  begin
    p := items.Delete(idx);
    sql += '    ' + statement(p) + LineEnding;
    Count += 1;
    if Count = 256 then
    begin
      task := TTask.Create();
      task.runner := ExecuteBlock(sql, Count);
      activeTasks.add(task);
      Count := 0;
      sql := '';
    end;
  end;
  if Count > 0 then
  begin
    task := TTask.Create();
    task.runner := ExecuteBlock(sql, Count);
    activeTasks.add(task);
  end;
  while activeTasks.workingCount > 0 do
    Sleep(10);
  activeTasks.Stop;
  activeTasks.Terminate;
  FreeAndNil(activeTasks);
  FConnection.Commit;
end;

{ TFirebirdDatabaseWriter }

procedure TFirebirdDatabaseWriter<TItemType>.SetTableName(AValue: string);
begin
  if FTableName = AValue then Exit;
  FTableName := AValue;
end;

function TFirebirdDatabaseWriter<TItemType>.Open: boolean;
var
  RS: IZResultSet;
begin
  Result := inherited Open;
  RS := FConnection.CreateStatement.ExecuteQuery('SELECT r.RDB$INDEX_NAME FROM RDB$INDICES r WHERE r.RDB$INDEX_INACTIVE = 0 and RDB$RELATION_NAME = ' + QuotedStr(TableName));
  while RS.Next do
  begin
    FConnection.CreateStatement.Execute('ALTER INDEX ' + RS.GetAnsiString(1) + ' INACTIVE');
  end;
  FConnection.Commit;
end;

function TFirebirdDatabaseWriter<TItemType>.Close: boolean;
var
  RS: IZResultSet;
begin
  RS := FConnection.CreateStatement.ExecuteQuery('SELECT r.RDB$INDEX_NAME FROM RDB$INDICES r WHERE r.RDB$INDEX_INACTIVE = 0 and RDB$RELATION_NAME = ' + QuotedStr(TableName));
  while RS.Next do
  begin
    FConnection.CreateStatement.Execute('ALTER INDEX ' + RS.GetAnsiString(1) + ' ACTIVE');
  end;
  FConnection.Commit;
  Result := inherited Close;
end;


end.
