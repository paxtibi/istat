unit istat.batch.popolazione;

{$mode delphi}{$H+}
interface

uses
  Classes, SysUtils, istat.batch, ZDbcIntfs, paxutils;

type
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

  TPopolazioneProcessor = class(TStringAbstractItemProcessor<TIstatPopolazioneRecord>)
    function process(const aIntput: string): TIstatPopolazioneRecord; override;
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
    function getWriterDecessi: IItemWriter<TIstatPopolazioneRecord>;
  public
    constructor Create(aFileName: TFileName; aConnection: IZConnection; aReaderListener: IItemReaderListener; aWriterListener: IItemWriterListener);
    procedure run;
  end;

implementation

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

function TPopolazioneRunner.getWriterDecessi: IItemWriter<TIstatPopolazioneRecord>;
var
  writer: TPopolazioneDatabaseIstatDecessiWriter;
begin
  writer := TPopolazioneDatabaseIstatDecessiWriter.Create;
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
  processore: IItemProcessor<string, TIstatPopolazioneRecord>;
  writer: IItemWriter<TIstatPopolazioneRecord>;
  item: TIstatPopolazioneRecord;
  line: string = '';
  startTime: uint64 = 0;
begin
  startTime := millis;
  processore := TPopolazioneProcessor.Create;
  reader := getReaderDecessi;
  writer := getWriterDecessi;
  reader.Open;
  writer.Open;
  Writeln(Format('Prepared popolazione in %s', [millisToString(millis() - startTime)], DefaultFormatSettings));
  reader.Read(line); // Skip Header
  while reader.Read(line) do
  begin
    line := StringReplace(line, '|', ',', [rfReplaceAll]);
    item := processore.process(line);
    writer.Write(item);
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
  Result := inherited Close;
end;

{ TPopolazioneProcessor }

function TPopolazioneProcessor.process(const aIntput: string): TIstatPopolazioneRecord;
var
  cursor: PChar;
begin
  cursor := PChar(aIntput);
  with Result do
  begin
    ITTER107 := Next(Cursor);
    Territorio := Next(Cursor);
    TIPO_DATO15 := Next(Cursor);
    Tipo_di_indicatore_demografico := Next(Cursor);
    SEXISTAT1 := Next(Cursor);
    Sesso := Next(Cursor);
    ETA1 := Next(Cursor);
    Eta := Next(Cursor);
    STATCIV2 := Next(Cursor);
    Stato_civile := Next(Cursor);
    TIME := Next(Cursor);
    Seleziona_periodo := Next(Cursor);
    Value := Next(Cursor);
    Flag_Codes := Next(Cursor);
    Flags := Next(Cursor);
  end;
end;


end.
