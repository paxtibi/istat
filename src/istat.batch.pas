unit istat.batch;

{$mode delphi}{$H+}

interface

uses
  Classes, SysUtils, ZDbcIntfs, fgl;

type
  IItemReaderListener = interface
    ['{7F17693E-85E3-4838-A55B-F4B327D6E59F}']
    procedure readCount;
    function Count: uint64;
    function totalCount: uint64;
  end;

  IItemWriterListener = interface
    ['{46CC8B66-9258-4B56-88EA-E808C031BCDD}']
    procedure writeCount;
    function Count: uint64;
    function totalCount: uint64;
  end;

  IItemReader<TItemType> = interface
    ['{E7FC1E44-7907-421C-8403-B5333714CEC2}']
    function Open: boolean;
    function Read(var item: TItemType): boolean;
    function Close: boolean;
    procedure setListener(listener: IItemReaderListener);
  end;

  IStringReader = interface(IItemReader<string>)
  end;

  IItemWriter<TItemType> = interface
    ['{E7FC1E44-7907-421C-8403-B5333714CEC2}']
    function Open: boolean;
    procedure Write(const item: TItemType);
    function Close: boolean;
    procedure setListener(listener: IItemWriterListener);
  end;

  IChunk<TItemType> = interface
    ['{32254F0A-A303-45C5-AD6E-507F715EBB98}']
    function Count: integer;
    function add(const item: TItemType): boolean;
    function get(const index: integer): TItemType;
  end;

  IChunkItemWriter<TItemType> = interface
    ['{E59FC978-116E-4D0E-A089-476DF7EF44C7}']
    function Open: boolean;
    procedure Write(const item: IChunk<TItemType>);
    function Close: boolean;
    procedure setListener(listener: IItemWriterListener);
  end;

  IItemProcessor<TInputType, TOutputType> = interface
    ['{001C3B41-609E-4266-B860-2B687FB4304E}']
    function process(const aIntput: TInputType): TOutputType;
  end;

  IStep<TInputType, TOutputType> = interface
    ['{F6976823-E7E4-4841-A05B-B47CFD8F1E23}']
    function getItemProcessor: IItemProcessor<TInputType, TOutputType>;
    function getItemReader: IItemReader<TInputType>;
    function getItemWriter: IItemWriter<TOutputType>;
    procedure setItemProcessor(aItem: IItemProcessor<TInputType, TOutputType>);
    procedure setItemReader(aItem: IItemReader<TInputType>);
    procedure setItemWriter(aItem: IItemWriter<TOutputType>);
  end;

  { TAbstractItemReader }

  TAbstractItemReader<TItemType> = class(TInterfacedObject, IItemReader<TItemType>)
  protected
    FListener: IItemReaderListener;
  public
    function Open: boolean; virtual; abstract;
    function Read(var item: TItemType): boolean; virtual; abstract;
    function Close: boolean; virtual; abstract;
    procedure setListener(listener: IItemReaderListener);
  end;

  { TAbstractItemWriter }

  TAbstractItemWriter<TItemType> = class(TInterfacedObject, IItemWriter<TItemType>)
  protected
    FListener: IItemWriterListener;
  public
    function Open: boolean; virtual; abstract;
    procedure Write(const item: TItemType); virtual; abstract;
    function Close: boolean; virtual; abstract;
    procedure setListener(listener: IItemWriterListener);
  end;

  { TAbstractChunkItemWriter }

  TAbstractChunkItemWriter<TItemType> = class(TInterfacedObject, IChunkItemWriter<TItemType>)
  protected
    FListener: IItemWriterListener;
  public
    function Open: boolean; virtual; abstract;
    procedure Write(const item: IChunk<TItemType>); virtual; abstract;
    function Close: boolean; virtual; abstract;
    procedure setListener(listener: IItemWriterListener);
  end;

  { TFlatFileReader }

  TFlatFileReader = class(TAbstractItemReader<string>, IStringReader)
  protected
    FFile: Text;
    FFileName: string;
    procedure SetFileName(AValue: string);
  public
    function Open: boolean; override;
    function Read(var item: string): boolean; override;
    function Close: boolean; override;
  public
    property FileName: string read FFileName write SetFileName;
  end;

  { TAbstractDatabaseWriter }

  TAbstractDatabaseWriter<TItemType> = class(TAbstractItemWriter<TItemType>)
  protected
    FConnection: IZConnection;
    procedure SetConnection(AValue: IZConnection);
  public
    function Open: boolean; override;
    function Close: boolean; override;
  public
    property Connection: IZConnection read FConnection write SetConnection;
  end;

  { TAbstractChunkedDatabaseWriter }

  TAbstractChunkedDatabaseWriter<TItemType> = class(TAbstractChunkItemWriter<TItemType>)
  protected
    FConnection: IZConnection;
    procedure SetConnection(AValue: IZConnection);
  public
    function Open: boolean; override;
    function Close: boolean; override;
  public
    property Connection: IZConnection read FConnection write SetConnection;
  end;

  { TFirebirdChunkedDatabaseWriter }

  TFirebirdChunkedDatabaseWriter<TItemType> = class(TAbstractChunkedDatabaseWriter<TItemType>)
  protected
    function statement(item: TItemType): rawbytestring; virtual; abstract;
    function getTableName: string;
  public
    function Open: boolean; override;
    function Close: boolean; override;
    procedure Write(const items: IChunk<TItemType>); override;
  end;

  TBaseChunk<TItemType> = class(TInterfacedObject, IChunk<TItemType>)
  protected
    fChunk: TFPGList<TItemType>;
  public
    procedure AfterConstruction; override;
    procedure BeforeDestruction; override;

    function Count: integer; virtual;
    function add(const item: TItemType): boolean; virtual;
    function get(const index: integer): TItemType; virtual;
  end;

  { TStringAbstractItemProcessor }

  TStringAbstractItemProcessor<OutputItem> = class(TInterfacedObject, IItemProcessor<string, OutputItem>)
  protected
    function Next(var cursor: PChar; separator: char = ','): string;
  public
    function process(const aIntput: string): OutputItem; virtual; abstract;
  end;

  { TAbstractStep }

  TAbstractStep<TInputType, TOutputType> = class(TInterfacedObject, IStep<TInputType, TOutputType>)
  protected
    fProcessor: IItemProcessor<TInputType, TOutputType>;
    fReader: IItemReader<TInputType>;
    fWriter: IItemWriter<TOutputType>;
  public
    function getItemProcessor: IItemProcessor<TInputType, TOutputType>;
    function getItemReader: IItemReader<TInputType>;
    function getItemWriter: IItemWriter<TOutputType>;
    procedure setItemProcessor(aItem: IItemProcessor<TInputType, TOutputType>);
    procedure setItemReader(aItem: IItemReader<TInputType>);
    procedure setItemWriter(aItem: IItemWriter<TOutputType>);
  end;

  { TItemReaderListener }

  TItemReaderListener = class(TInterfacedObject, IItemReaderListener)
  protected
    FCount: uint64;
    fLock: TRTLCriticalSection;
  public
    constructor Create;
    destructor Destroy; override;
    procedure readCount;
    function Count: uint64;
    function totalCount: uint64;
  end;

  { TItemWriterListener }

  TItemWriterListener = class(TInterfacedObject, IItemWriterListener)
  protected
    FCount: uint64;
    fLock: TRTLCriticalSection;
  public
    constructor Create;
    destructor Destroy; override;
    procedure writeCount;
    function Count: uint64;
    function totalCount: uint64;
  end;

implementation

{ TAbstractDatabaseWriter }

procedure TAbstractDatabaseWriter<TItemType>.SetConnection(AValue: IZConnection);
begin
  if FConnection = AValue then Exit;
  FConnection := AValue;
end;

function TAbstractDatabaseWriter<TItemType>.Open: boolean;
begin
  FConnection.Open;
  Result := True;
end;

function TAbstractDatabaseWriter<TItemType>.Close: boolean;
begin
  if not FConnection.IsClosed then
    FConnection.Close;
  Result := True;
end;

{ TStringAbstractItemProcessor }

function TStringAbstractItemProcessor<OutputItem>.Next(var cursor: PChar; separator: char): string;
begin
  Result := '';
  while (cursor^ <> #0) do
  begin
    case cursor^ of
      '"': begin
        Inc(cursor);
        while not (cursor^ in ['"', #0]) do
        begin
          Result += cursor^;
          Inc(cursor);
        end;
        if cursor^ = '"' then Inc(cursor);
        if cursor^ = separator then Inc(cursor);
        exit(Result);
      end;
      else
      begin
        if cursor^ = separator then
        begin
          Inc(cursor);
          exit(Result);
        end
        else
          Result += cursor^;
      end;
    end;
    Inc(cursor);
  end;
end;

{ TAbstractItemReader }

procedure TAbstractItemReader<TItemType>.setListener(listener: IItemReaderListener);
begin
  FListener := listener;
end;

{ TAbstractItemWriter }

procedure TAbstractItemWriter<TItemType>.setListener(listener: IItemWriterListener);
begin
  FListener := listener;
end;

{ TAbstractStep }

function TAbstractStep<TInputType, TOutputType>.getItemProcessor: IItemProcessor<TInputType, TOutputType>;
begin
  Result := fProcessor;
end;

function TAbstractStep<TInputType, TOutputType>.getItemReader: IItemReader<TInputType>;
begin
  Result := fReader;
end;

function TAbstractStep<TInputType, TOutputType>.getItemWriter: IItemWriter<TOutputType>;
begin
  Result := fWriter;
end;

procedure TAbstractStep<TInputType, TOutputType>.setItemProcessor(aItem: IItemProcessor<TInputType, TOutputType>);
begin
  fProcessor := aItem;
end;

procedure TAbstractStep<TInputType, TOutputType>.setItemReader(aItem: IItemReader<TInputType>);
begin
  fReader := aItem;
end;

procedure TAbstractStep<TInputType, TOutputType>.setItemWriter(aItem: IItemWriter<TOutputType>);
begin
  fWriter := aItem;
end;

{ TAbstractChunkItemWriter }

procedure TAbstractChunkItemWriter<TItemType>.setListener(listener: IItemWriterListener);
begin
  FListener := listener;
end;

{ TAbstractChunkedDatabaseWriter }

procedure TAbstractChunkedDatabaseWriter<TItemType>.SetConnection(AValue: IZConnection);
begin
  FConnection := AValue;
end;

function TAbstractChunkedDatabaseWriter<TItemType>.Open: boolean;
begin
  FConnection.Open;
  Result := True;
end;

function TAbstractChunkedDatabaseWriter<TItemType>.Close: boolean;
begin
  if not FConnection.IsClosed then
    FConnection.Close;
  Result := True;
end;

function TFirebirdChunkedDatabaseWriter<TItemType>.getTableName: string;
begin

end;

function TFirebirdChunkedDatabaseWriter<TItemType>.Open: boolean;
var
  RS: IZResultSet;
begin
  Result := inherited Open;
  RS := FConnection.CreateStatement.ExecuteQuery('SELECT r.RDB$INDEX_NAME FROM RDB$INDICES r WHERE r.RDB$INDEX_INACTIVE = 0 and RDB$RELATION_NAME = ''' + getTableName + '''');
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
  RS := FConnection.CreateStatement.ExecuteQuery('SELECT r.RDB$INDEX_NAME FROM RDB$INDICES r WHERE r.RDB$INDEX_INACTIVE = 0 and RDB$RELATION_NAME = ''' + getTableName + '''');
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
begin
  sql := 'execute block' + LineEnding + // 0
    'as' + LineEnding +// 0
    '  begin' + LineEnding;
  for idx := 0 to items.Count - 1 do
  begin
    sql += '    ' + statement(items.get(idx)) + LineEnding;
    FListener.writeCount;
  end;
  // 0
  sql += '  end' + LineEnding +// 0
    ';';
  try
    if FConnection.CreateStatement.Execute(sql) then
    begin
      FConnection.Commit;
    end;
  except
    On  E: Exception do
    begin
      Writeln(E.Message);
    end;
  end;
end;


procedure TBaseChunk<TItemType>.AfterConstruction;
begin
  inherited AfterConstruction;
  fChunk := TFPGList<TItemType>.Create;
end;

procedure TBaseChunk<TItemType>.BeforeDestruction;
begin
  FreeAndNil(fChunk);
  inherited BeforeDestruction;
end;

function TBaseChunk<TItemType>.Count: integer;
begin
  Result := fChunk.Count;
end;

function TBaseChunk<TItemType>.add(const item: TItemType): boolean;
begin
  fChunk.add(item);
end;

function TBaseChunk<TItemType>.get(const index: integer): TItemType;
begin
  Result := fChunk[index];
end;

{ TItemWriterListener }

constructor TItemWriterListener.Create;
begin
  InitCriticalSection(fLock);
end;

destructor TItemWriterListener.Destroy;
begin
  DoneCriticalSection(fLock);
  inherited Destroy;
end;

procedure TItemWriterListener.writeCount;
begin
  try
    EnterCriticalSection(fLock);
    fCount += 1;
  finally
    LeaveCriticalSection(fLock);
  end;

end;

function TItemWriterListener.Count: uint64;
const
  lastCount: uint64 = 0;
begin
  Result := FCount - lastCount;
  lastCount := FCount;
end;

function TItemWriterListener.totalCount: uint64;
begin
  Result := FCount;
end;

{ TItemReaderListener }

constructor TItemReaderListener.Create;
begin
  InitCriticalSection(fLock);
end;

destructor TItemReaderListener.Destroy;
begin
  DoneCriticalSection(fLock);
  inherited Destroy;
end;

procedure TItemReaderListener.readCount;
begin
  try
    EnterCriticalSection(fLock);
    fCount += 1;
  finally
    LeaveCriticalSection(fLock);
  end;
end;

function TItemReaderListener.Count: uint64;
const
  lastCount: uint64 = 0;
begin
  Result := FCount - lastCount;
  lastCount := FCount;
end;

function TItemReaderListener.totalCount: uint64;
begin
  Result := FCount;
end;

{ TFlatFileReader }

procedure TFlatFileReader.SetFileName(AValue: string);
begin
  if FFileName = AValue then Exit;
  FFileName := AValue;
end;

function TFlatFileReader.Open: boolean;
begin
  AssignFile(FFile, FFileName);
  Reset(FFile);
  Result := True;
end;

function TFlatFileReader.Read(var item: string): boolean;
begin
  Result := False;
  if not EOF(FFile) then
  begin
    ReadLn(FFile, item);
    Result := True;
    if FListener <> nil then
    begin
      FListener.readCount;
    end;
  end;
end;

function TFlatFileReader.Close: boolean;
begin
  CloseFile(FFile);
  Result := True;
end;

end.
