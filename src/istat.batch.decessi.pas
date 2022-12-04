unit istat.batch.decessi;

{$mode delphi}{$H+}

interface

uses
  Classes, SysUtils, istat.batch, istat.batch.firebird, ZDbcIntfs, paxutils;

type
  PIstatDecessiRecord = ^TIstatDecessiRecord;

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

  TDecessiProcessor = class(TStringAbstractItemProcessor<PIstatDecessiRecord>)
    function process(const aIntput: string): PIstatDecessiRecord; override;
  end;

  { TDecessiDatabaseIstatDecessiWriter }

  TDecessiDatabaseIstatDecessiWriter = class(TFirebirdDatabaseWriter<TIstatDecessiRecord>)
  public
    function Open: boolean; override;
    procedure Write(const item: TIstatDecessiRecord); override;
  end;

  { TChunkedDecessiDatabaseIstatDecessiWriter }

  TChunkedDecessiDatabaseIstatDecessiWriter = class(TFirebirdChunkedDatabaseWriter<PIstatDecessiRecord>)
  protected
    function statement(item: PIstatDecessiRecord): rawbytestring; override;
    function getTableName: string; override;
  end;

  { TDecessiRunner }

  TDecessiRunner = class(TFirebirdStepRunner<PIstatDecessiRecord>)
  protected
    function getReaderDecessi: IStringReader; override;
    function getWriterDecessi: IChunkItemWriter<PIstatDecessiRecord>; override;
    function getProcessor: IStringProcessor<PIstatDecessiRecord>; override;
  end;

implementation

{ TChunkedDecessiDatabaseIstatDecessiWriter }

function TChunkedDecessiDatabaseIstatDecessiWriter.statement(item: PIstatDecessiRecord): rawbytestring;
begin
  with item^ do
    Result := Format('INSERT INTO ISTAT_DECESSI VALUES(%s,%s,%s,%s,%s, %s,%s,%s,%s,%s, %s,%s,%s,%s,%s, %s,%s,%s,%s,%s, %s,%s,%s,%s,%s ,%s,%s,%s,%s);', [QuotedStr(ITTER107), QuotedStr(Territorio), QuotedStr(TIPO_DATO15), QuotedStr(Tipo_dato), QuotedStr(ETA1_A), QuotedStr(Eta), QuotedStr(SEXISTAT1), QuotedStr(Sesso), QuotedStr(STATCIV2), QuotedStr(Stato_civile), QuotedStr(TITOLO_STUDIO), QuotedStr(Istruzione), QuotedStr(T_BIS_A), QuotedStr(Mese_di_decesso), QuotedStr(T_BIS_B), QuotedStr(Anno_di_nascita), QuotedStr(ETA1_B), QuotedStr(Classe_di_eta_coniuge_superstite), QuotedStr(T_BIS_C), QuotedStr(Anno_di_matrimonio), QuotedStr(ISO), QuotedStr(Paese_di_cittadinanza), QuotedStr(CAUSEMORTE_SL), QuotedStr(Causa_iniziale_di_morte), QuotedStr(TIME), QuotedStr(Seleziona_periodo), QuotedStr(Value), QuotedStr(Flag_Codes), QuotedStr(Flags)]);
end;

function TChunkedDecessiDatabaseIstatDecessiWriter.getTableName: string;
begin
  Result := 'ISTAT_DECESSI';
end;

{ TDecessiRunner }

function TDecessiRunner.getReaderDecessi: IStringReader;
var
  reader: TFlatFileReader;
begin
  reader := TFlatFileReader.Create;
  reader.setListener(FReaderListener);
  reader.FileName := FFileName;
  Result := reader;
end;

function TDecessiRunner.getWriterDecessi: IChunkItemWriter<PIstatDecessiRecord>;
var
  writer: TChunkedDecessiDatabaseIstatDecessiWriter;
begin
  writer := TChunkedDecessiDatabaseIstatDecessiWriter.Create;
  writer.setListener(FWriterListener);
  writer.Connection := FConnection;
  Result := writer;
end;

function TDecessiRunner.getProcessor: IStringProcessor<PIstatDecessiRecord>;
begin
  Result := TDecessiProcessor.Create;
end;

{ TDecessiDatabaseIstatDecessiWriter }

function TDecessiDatabaseIstatDecessiWriter.Open: boolean;
begin
  Result := inherited Open;
  FStatement := FConnection.PrepareStatement('INSERT INTO ISTAT_DECESSI VALUES(?,?,?,?,? ,?,?,?,?,? ,?,?,?,?,? ,?,?,?)');
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

{ TDecessiProcessor }

function TDecessiProcessor.process(const aIntput: string): PIstatDecessiRecord;
var
  cursor: PChar;
begin
  cursor := PChar(aIntput);
  New(Result);
  with Result^ do
  begin
    ITTER107 := Next(Cursor, '|');
    Territorio := Next(Cursor, '|');
    TIPO_DATO15 := Next(Cursor, '|');
    Tipo_dato := Next(Cursor, '|');
    ETA1_A := Next(Cursor, '|');
    Eta := Next(Cursor, '|');
    SEXISTAT1 := Next(Cursor, '|');
    Sesso := Next(Cursor, '|');
    STATCIV2 := Next(Cursor, '|');
    Stato_civile := Next(Cursor, '|');
    TITOLO_STUDIO := Next(Cursor, '|');
    Istruzione := Next(Cursor, '|');
    T_BIS_A := Next(Cursor, '|');
    Mese_di_decesso := Next(Cursor, '|');
    T_BIS_B := Next(Cursor, '|');
    Anno_di_nascita := Next(Cursor, '|');
    ETA1_B := Next(Cursor, '|');
    Classe_di_eta_coniuge_superstite := Next(Cursor, '|');
    T_BIS_C := Next(Cursor, '|');
    Anno_di_matrimonio := Next(Cursor, '|');
    ISO := Next(Cursor, '|');
    Paese_di_cittadinanza := Next(Cursor, '|');
    CAUSEMORTE_SL := Next(Cursor, '|');
    Causa_iniziale_di_morte := Next(Cursor, '|');
    TIME := Next(Cursor, '|');
    Seleziona_periodo := Next(Cursor, '|');
    Value := Next(Cursor, '|');
    Flag_Codes := Next(Cursor, '|');
    Flags := Next(Cursor, '|');
  end;
end;

end.
