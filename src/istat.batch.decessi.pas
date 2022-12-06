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

  TDecessiProcessor = class(TStringAbstractItemProcessor<TIstatDecessiRecord>)
    function process(const aIntput: string): TIstatDecessiRecord; override;
  end;

  { TDecessiDatabaseIstatWriter }

  TDecessiDatabaseIstatWriter = class(TFirebirdDatabaseWriter<TIstatDecessiRecord>)
  public
    procedure AfterConstruction; override;
    function Open: boolean; override;
    procedure Write(const item: TIstatDecessiRecord); override;
  end;

  { TChunkedDecessiDatabaseIstatDecessiWriter }

  TChunkedDecessiDatabaseIstatDecessiWriter = class(TFirebirdChunkedDatabaseWriter<TIstatDecessiRecord>)
  protected
    function statement(item: TIstatDecessiRecord): rawbytestring; override;
    function getTableName: string; override;
  end;

  { TDecessiRunner }

  TDecessiRunner = class(TFirebirdStepRunner<TIstatDecessiRecord>)
  protected
    function getReaderDecessi: IStringReader; override;
    function getWriterDecessi: IItemWriter<TIstatDecessiRecord>; override;
    function getProcessor: IStringProcessor<TIstatDecessiRecord>; override;
  end;

implementation

{ TChunkedDecessiDatabaseIstatDecessiWriter }

function TChunkedDecessiDatabaseIstatDecessiWriter.statement(item: TIstatDecessiRecord): rawbytestring;
begin
  with item do
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

function TDecessiRunner.getWriterDecessi: IItemWriter<TIstatDecessiRecord>;
var
  writer: TDecessiDatabaseIstatWriter;
begin
  writer := TDecessiDatabaseIstatWriter.Create;
  writer.setListener(FWriterListener);
  writer.Connection := FConnection;
  Result := writer;
end;

function TDecessiRunner.getProcessor: IStringProcessor<TIstatDecessiRecord>;
begin
  Result := TDecessiProcessor.Create;
end;

{ TDecessiDatabaseIstatWriter }

procedure TDecessiDatabaseIstatWriter.AfterConstruction;
begin
  inherited AfterConstruction;
  FTableName := 'ISTAT_DECESSI';
end;

function TDecessiDatabaseIstatWriter.Open: boolean;
begin
  Result := inherited Open;
  FStatement := FConnection.PrepareStatement('INSERT INTO ISTAT_DECESSI VALUES(?,?,?,?,?, ?,?,?,?,?, ?,?,?,?,?, ?,?,?,?,?, ?,?,?,?,? ,?,?,?,?)');
end;

procedure TDecessiDatabaseIstatWriter.Write(const item: TIstatDecessiRecord);
begin
  try
    with item do
    begin
      FStatement.SetString(01, ITTER107);
      FStatement.SetUnicodeString(02, Territorio);
      FStatement.SetString(03, TIPO_DATO15);
      FStatement.SetString(04, Tipo_dato);
      FStatement.SetString(05, ETA1_A);
      FStatement.SetString(06, Eta);
      FStatement.SetString(07, SEXISTAT1);
      FStatement.SetString(08, Sesso);
      FStatement.SetString(09, STATCIV2);
      FStatement.SetString(10, Stato_civile);
      FStatement.SetString(11, TITOLO_STUDIO);
      FStatement.SetString(12, Istruzione);
      FStatement.SetString(13, T_BIS_A);
      FStatement.SetString(14, Mese_di_decesso);
      FStatement.SetString(15, T_BIS_B);
      FStatement.SetString(16, Anno_di_nascita);
      FStatement.SetString(17, ETA1_B);
      FStatement.SetString(18, Classe_di_eta_coniuge_superstite);
      FStatement.SetString(19, T_BIS_C);
      FStatement.SetString(20, Anno_di_matrimonio);
      FStatement.SetString(21, ISO);
      FStatement.SetString(22, Paese_di_cittadinanza);
      FStatement.SetString(23, StringReplace(CAUSEMORTE_SL, '_', '.', [rfReplaceAll]));
      FStatement.SetString(24, Causa_iniziale_di_morte);
      FStatement.SetString(25, TIME);
      FStatement.SetString(26, Seleziona_periodo);
      if Value = '' then
        FStatement.SetNull(27, stInteger)
      else
        FStatement.SetInt(27, StrToInt(Value));
      FStatement.SetString(28, Flag_Codes);
      FStatement.SetString(29, Flags);
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

function TDecessiProcessor.process(const aIntput: string): TIstatDecessiRecord;
var
  cursor: PChar;
begin
  cursor := PChar(aIntput);
  with Result do
  begin
    ITTER107 := Next(Cursor, '|');
    Territorio := Next(Cursor, '|');
    TIPO_DATO15 := Next(Cursor, '|');
    Tipo_dato := Next(Cursor, '|');
    ETA1_A := Next(Cursor, '|');
    if ETA1_A = 'Y_GE95' then
      ETA1_A := '95';
    ETA1_A := StringReplace(ETA1_A, 'Y', '', []);
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
