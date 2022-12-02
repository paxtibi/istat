unit istat.batch.popolazione;

{$mode delphi}{$H+}
interface

uses
  Classes, SysUtils, istat.batch, ZDbcIntfs, paxutils;

type
  PIstatPopolazioneRecord = ^TIstatPopolazioneRecord;

  TIstatPopolazioneRecord = record
    ITTER107: string;
    Territorio: string;
    TIPO_DATO15: string;
    Tipo_di_indicatore_demografico: string;
    SEXISTAT1: string;
    Sesso: string;
    ETA1: string;
    Eta: string;
    STATCIV2: string;
    Stato_civile: string;
    TIME: string;
    Seleziona_periodo: string;
    Value: string;
    Flag_Codes: string;
    Flags: string;
  end;

  { TPopolazioneProcessor }

  TPopolazioneProcessor = class(TStringAbstractItemProcessor<PIstatPopolazioneRecord>)
    function process(const aIntput: string): PIstatPopolazioneRecord; override;
  end;

  { TPopolazioneDatabaseIstatDecessiWriter }

  TPopolazioneDatabaseIstatDecessiWriter = class(TAbstractDatabaseWriter<TIstatPopolazioneRecord>)
  protected
    FStatement: IZPreparedStatement;
  public
    function Open: boolean; override;
    procedure Write(const item: TIstatPopolazioneRecord); override;
    function Close: boolean; override;
  end;

  { TChunkedPopolazioneDatabaseIstatDecessiWriter }

  TChunkedPopolazioneDatabaseIstatDecessiWriter = class(TFirebirdChunkedDatabaseWriter<PIstatPopolazioneRecord>)
  protected
    function statement(item: PIstatPopolazioneRecord): rawbytestring; override;
    function getTableName: string;
  public
  end;

  { TPopolazioneRunner }

  TPopolazioneRunner = class(TInterfacedObject, IRunnable)
  private
    FConnection: IZConnection;
    FFileName: string;
    FReaderListener: IItemReaderListener;
    FWriterListener: IItemWriterListener;
    procedure SetConnection(AValue: IZConnection);
    procedure SetFileName(AValue: string);
  protected
    function getReaderDecessi: IStringReader;
    function getWriterDecessi: IChunkItemWriter<PIstatPopolazioneRecord>;
  public
    constructor Create(aFileName: TFileName; aConnection: IZConnection; aReaderListener: IItemReaderListener; aWriterListener: IItemWriterListener);
    procedure run;
  end;

implementation

{ TChunkedPopolazioneDatabaseIstatDecessiWriter }

function TChunkedPopolazioneDatabaseIstatDecessiWriter.statement(item: PIstatPopolazioneRecord): rawbytestring;
begin
  with item^ do
  begin
    Result := Format('INSERT INTO ISTAT_POPOLAZIONE VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);', [QuotedStr(ITTER107), QuotedStr(Territorio), QuotedStr(TIPO_DATO15), QuotedStr(Tipo_di_indicatore_demografico), QuotedStr(SEXISTAT1), QuotedStr(Sesso), QuotedStr(ETA1), QuotedStr(Eta), QuotedStr(STATCIV2), QuotedStr(Stato_civile), QuotedStr(TIME), QuotedStr(Seleziona_periodo), QuotedStr(Value), QuotedStr(Flag_Codes), QuotedStr(Flags)]);
  end;
end;

function TChunkedPopolazioneDatabaseIstatDecessiWriter.getTableName: string;
begin
  Result := 'ISTAT_POPOLAZIONE';
end;

{ TPopolazioneRunner }

procedure TPopolazioneRunner.SetConnection(AValue: IZConnection);
begin
  FConnection := AValue;
end;

procedure TPopolazioneRunner.SetFileName(AValue: string);
begin
  FFileName := AValue;
end;

function TPopolazioneRunner.getReaderDecessi: IStringReader;
var
  reader: TFlatFileReader;
begin
  reader := TFlatFileReader.Create;
  reader.FileName := FFileName;
  reader.setListener(FReaderListener);
  Result := reader;
end;

function TPopolazioneRunner.getWriterDecessi: IChunkItemWriter<PIstatPopolazioneRecord>;
var
  writer: TChunkedPopolazioneDatabaseIstatDecessiWriter;
begin
  writer := TChunkedPopolazioneDatabaseIstatDecessiWriter.Create;
  writer.Connection := FConnection;
  writer.setListener(FWriterListener);
  Result := writer;
end;

constructor TPopolazioneRunner.Create(aFileName: TFileName; aConnection: IZConnection; aReaderListener: IItemReaderListener; aWriterListener: IItemWriterListener);
begin
  FConnection := aConnection;
  FFileName := aFileName;
  FReaderListener := aReaderListener;
  FWriterListener := aWriterListener;
end;

procedure TPopolazioneRunner.run;
var
  reader: IItemReader<string>;
  processore: IItemProcessor<string, PIstatPopolazioneRecord>;
  writer: IChunkItemWriter<PIstatPopolazioneRecord>;
  item: PIstatPopolazioneRecord;
  line: string = '';
  startTime: uint64 = 0;
  chunk: TBaseChunk<PIstatPopolazioneRecord>;
  index: integer;
begin
  startTime := millis;
  processore := TPopolazioneProcessor.Create;
  reader := getReaderDecessi;
  writer := getWriterDecessi;
  reader.Open;
  writer.Open;
  Writeln(Format('Prepared popolazione in %s', [millisToString(millis() - startTime)], DefaultFormatSettings));
  reader.Read(line);
  while True do
  begin
    chunk := TBaseChunk<PIstatPopolazioneRecord>.Create;
    index := 0;
    while index < 256 do
    begin
      if reader.Read(line) then
      begin
        item := processore.process(line);
        chunk.add(item);
        Inc(index);
      end
      else
        break;
    end;
    writer.Write(chunk);
    FreeAndNil(chunk);
  end;
  reader.Close;
  writer.Close;
  FConnection.Commit;
  FConnection := nil;
end;


{ TPopolazioneDatabaseIstatDecessiWriter }

function TPopolazioneDatabaseIstatDecessiWriter.Open: boolean;
var
  RS: IZResultSet;
begin
  Result := inherited Open;
  FConnection.Commit;
  RS := FConnection.CreateStatement.ExecuteQuery('SELECT r.RDB$INDEX_NAME FROM RDB$INDICES r WHERE r.RDB$INDEX_INACTIVE = 0 and RDB$RELATION_NAME = ''ISTAT_POPOLAZIONE''');
  while RS.Next do
  begin
    FConnection.CreateStatement.Execute('ALTER INDEX ' + RS.GetAnsiString(1) + ' INACTIVE');
  end;
  FStatement := FConnection.PrepareStatement('INSERT INTO ISTAT_POPOLAZIONE VALUES(?,?,?,?,?, ?,?,?,?,?, ?,?,?,?,?)');
  FConnection.Commit;
end;

procedure TPopolazioneDatabaseIstatDecessiWriter.Write(const item: TIstatPopolazioneRecord);
begin
  try
    with item do
    begin
      FStatement.SetString(1, ITTER107);
      FStatement.SetUnicodeString(2, Territorio);
      FStatement.SetString(3, TIPO_DATO15);
      FStatement.SetUnicodeString(4, Tipo_di_indicatore_demografico);
      FStatement.SetString(5, SEXISTAT1);
      FStatement.SetString(6, Sesso);
      FStatement.SetString(7, ETA1);
      FStatement.SetString(8, Eta);
      FStatement.SetString(9, STATCIV2);
      FStatement.SetString(10, Stato_civile);
      FStatement.SetString(11, TIME);
      FStatement.SetString(12, Seleziona_periodo);
      if Value = '' then
        FStatement.SetNull(13, stInteger)
      else
        FStatement.SetInt(13, StrToInt(Value));
      FStatement.SetString(14, Flag_Codes);
      FStatement.SetString(15, Flags);
    end;
    FStatement.ExecutePrepared;
    if (FListener <> nil) then
      FListener.writeCount;
  except
    on E: Exception do
    begin
      WriteLn(e.Message);
    end;
  end;
end;

function TPopolazioneDatabaseIstatDecessiWriter.Close: boolean;
var
  RS: IZResultSet;
begin
  RS := FConnection.CreateStatement.ExecuteQuery('SELECT r.RDB$INDEX_NAME FROM RDB$INDICES r WHERE r.RDB$INDEX_INACTIVE = 0 and RDB$RELATION_NAME = ''ISTAT_DECESSI''');
  while RS.Next do
  begin
    FConnection.CreateStatement.Execute('ALTER INDEX ' + RS.GetAnsiString(1) + ' ACTIVE');
  end;
  FConnection.Commit;
  Result := inherited Close;
end;

{ TPopolazioneProcessor }

function TPopolazioneProcessor.process(const aIntput: string): PIstatPopolazioneRecord;
var
  cursor: PChar;
begin
  New(Result);
  cursor := PChar(aIntput);
  with Result^ do
  begin
    ITTER107 := Next(Cursor, '|');
    Territorio := Next(Cursor, '|');
    TIPO_DATO15 := Next(Cursor, '|');
    Tipo_di_indicatore_demografico := Next(Cursor, '|');
    SEXISTAT1 := Next(Cursor, '|');
    Sesso := Next(Cursor, '|');
    ETA1 := Next(Cursor, '|');
    Eta := Next(Cursor, '|');
    STATCIV2 := Next(Cursor, '|');
    Stato_civile := Next(Cursor, '|');
    TIME := Next(Cursor, '|');
    Seleziona_periodo := Next(Cursor, '|');
    Value := Next(Cursor, '|');
    Flag_Codes := Next(Cursor, '|');
    Flags := Next(Cursor, '|');
  end;
end;


end.
