unit istat.batch.decessi;

{$mode delphi}{$H+}

interface

uses
  Classes, SysUtils, istat.batch, ZDbcIntfs, paxutils;

type
  TIstatDecessiRecord = record
    ITTER107: string;
    Territorio: string;
    TIPO_DATO15: string;
    Tipo_dato: string;
    ETA1_A: string;
    Eta: string;
    SEXISTAT1: string;
    Sesso: string;
    STATCIV2: string;
    Stato_civile: string;
    TITOLO_STUDIO: string;
    Istruzione: string;
    T_BIS_A: string;
    Mese_di_decesso: string;
    T_BIS_B: string;
    Anno_di_nascita: string;
    ETA1_B: string;
    Classe_di_eta_coniuge_superstite: string;
    T_BIS_C: string;
    Anno_di_matrimonio: string;
    ISO: string;
    Paese_di_cittadinanza: string;
    CAUSEMORTE_SL: string;
    Causa_iniziale_di_morte: string;
    TIME: string;
    Seleziona_periodo: string;
    Value: string;
    Flag_Codes: string;
    Flags: string;
  end;

  { TDecessiProcessor }

  TDecessiProcessor = class(TStringAbstractItemProcessor<TIstatDecessiRecord>)
    function process(const aIntput: string): TIstatDecessiRecord; override;
  end;

  { TDecessiDatabaseIstatDecessiWriter }

  TDecessiDatabaseIstatDecessiWriter = class(TAbstractDatabaseWriter<TIstatDecessiRecord>)
  protected
    FStatement: IZPreparedStatement;
  public
    function Open: boolean; override;
    procedure Write(const item: TIstatDecessiRecord); override;
    function Close: boolean; override;
  end;


  { TDecessiRunner }

  TDecessiRunner = class(TInterfacedObject, IRunnable)
  private
    FConnection: IZConnection;
    FFileName: string;
    FReaderListener: IItemReaderListener;
    FWriterListener: IItemWriterListener;
    procedure SetConnection(AValue: IZConnection);
    procedure SetFileName(AValue: string);
  protected
    function getReaderDecessi: IStringReader;
    function getWriterDecessi: IItemWriter<TIstatDecessiRecord>;
  public
    constructor Create(aFileName: TFileName; aConnection: IZConnection; aReaderListener: IItemReaderListener; aWriterListener: IItemWriterListener);
    procedure run;
  end;

implementation

{ TDecessiRunner }

procedure TDecessiRunner.SetConnection(AValue: IZConnection);
begin
  if FConnection = AValue then Exit;
  FConnection := AValue;
end;

procedure TDecessiRunner.SetFileName(AValue: string);
begin
  if FFileName = AValue then Exit;
  FFileName := AValue;
end;

function TDecessiRunner.getReaderDecessi: IStringReader;
var
  reader: TFlatFileReader;
begin
  reader := TFlatFileReader.Create;
  reader.setListener(FReaderListener);
  reader.FileName := FFileName;
  Result := reader;
end;

function TDecessiRunner.getWriterDecessi: IItemWriter<TIstatDecessiRecord>;
var
  writer: TDecessiDatabaseIstatDecessiWriter;
begin
  writer := TDecessiDatabaseIstatDecessiWriter.Create;
  writer.setListener(FWriterListener);
  writer.Connection := FConnection;
  Result := writer;
end;

constructor TDecessiRunner.Create(aFileName: TFileName; aConnection: IZConnection; aReaderListener: IItemReaderListener; aWriterListener: IItemWriterListener);
begin
  FFileName := aFileName;
  FConnection := aConnection;
  FWriterListener := aWriterListener;
  FReaderListener := aReaderListener;
end;

procedure TDecessiRunner.run;
var
  reader: IItemReader<string>;
  processore: IItemProcessor<string, TIstatDecessiRecord>;
  writer: IItemWriter<TIstatDecessiRecord>;
  item: TIstatDecessiRecord;
  line: string = '';
  startTime: uint64 = 0;
begin
  startTime := millis;
  processore := TDecessiProcessor.Create;
  reader := getReaderDecessi;
  writer := getWriterDecessi;
  reader.Open;
  writer.Open;
  Writeln(Format('Prepared decessi in %s', [millisToString(millis() - startTime)], DefaultFormatSettings));
  reader.Read(line);
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

{ TDecessiDatabaseIstatDecessiWriter }

function TDecessiDatabaseIstatDecessiWriter.Open: boolean;
var
  RS: IZResultSet;
begin
  Result := inherited Open;
  RS := FConnection.CreateStatement.ExecuteQuery('SELECT r.RDB$INDEX_NAME FROM RDB$INDICES r WHERE r.RDB$INDEX_INACTIVE = 0 and RDB$RELATION_NAME = ''ISTAT_DECESSI''');
  while RS.Next do
  begin
    FConnection.CreateStatement.Execute('ALTER INDEX ' + RS.GetAnsiString(1) + ' INACTIVE');
  end;
  FStatement := FConnection.PrepareStatement('INSERT INTO ISTAT_DECESSI VALUES(?,?,?,?,? ,?,?,?,?,? ,?,?,?,?,? ,?,?,?)');
  FConnection.Commit;
end;

procedure TDecessiDatabaseIstatDecessiWriter.Write(const item: TIstatDecessiRecord);
begin
  try
    with item do
    begin
      FStatement.SetString(1, ITTER107);
      FStatement.SetUnicodeString(2, Territorio);
      FStatement.SetString(3, TIPO_DATO15);
      FStatement.SetString(4, ETA1_A);
      FStatement.SetString(5, SEXISTAT1);
      FStatement.SetString(6, STATCIV2);
      FStatement.SetString(7, TITOLO_STUDIO);
      FStatement.SetString(8, T_BIS_A);
      FStatement.SetString(9, T_BIS_B);
      FStatement.SetString(10, Anno_di_nascita);
      FStatement.SetString(11, ETA1_B);
      FStatement.SetString(12, T_BIS_C);
      FStatement.SetString(13, Anno_di_matrimonio);
      FStatement.SetString(14, ISO);
      FStatement.SetString(15, CAUSEMORTE_SL);
      FStatement.SetString(16, TIME);
      FStatement.SetInt(17, StrToInt(Value));
      FStatement.SetString(18, Flag_Codes);
    end;
    FStatement.ExecutePrepared;
    if (FListener <> nil) then
      FListener.writeCount;
  except
    on E: Exception do
      Writeln(E.Message);
  end;
end;

function TDecessiDatabaseIstatDecessiWriter.Close: boolean;
var
  RS: IZResultSet;
begin
  FConnection.Commit;
  RS := FConnection.CreateStatement.ExecuteQuery('SELECT r.RDB$INDEX_NAME FROM RDB$INDICES r WHERE r.RDB$INDEX_INACTIVE = 1 and RDB$RELATION_NAME = ''ISTAT_DECESSI''');
  while RS.Next do
  begin
    FConnection.CreateStatement.Execute('ALTER INDEX ' + RS.GetAnsiString(1) + ' ACTIVE');
  end;
  Result := inherited Close;
end;

{ TDecessiProcessor }

function TDecessiProcessor.process(const aIntput: string): TIstatDecessiRecord;
var
  cursor: PChar;
begin
  cursor := PChar(aIntput);
  with Result do
  begin
    ITTER107 := Next(Cursor);
    Territorio := Next(Cursor);
    TIPO_DATO15 := Next(Cursor);
    Tipo_dato := Next(Cursor);
    ETA1_A := Next(Cursor);
    Eta := Next(Cursor);
    SEXISTAT1 := Next(Cursor);
    Sesso := Next(Cursor);
    STATCIV2 := Next(Cursor);
    Stato_civile := Next(Cursor);
    TITOLO_STUDIO := Next(Cursor);
    Istruzione := Next(Cursor);
    T_BIS_A := Next(Cursor);
    Mese_di_decesso := Next(Cursor);
    T_BIS_B := Next(Cursor);
    Anno_di_nascita := Next(Cursor);
    ETA1_B := Next(Cursor);
    Classe_di_eta_coniuge_superstite := Next(Cursor);
    T_BIS_C := Next(Cursor);
    Anno_di_matrimonio := Next(Cursor);
    ISO := Next(Cursor);
    Paese_di_cittadinanza := Next(Cursor);
    CAUSEMORTE_SL := Next(Cursor);
    Causa_iniziale_di_morte := Next(Cursor);
    TIME := Next(Cursor);
    Seleziona_periodo := Next(Cursor);
    Value := Next(Cursor);
    Flag_Codes := Next(Cursor);
    Flags := Next(Cursor);
  end;
end;

end.
