unit istat.batch.popolazione;

{$mode delphi}{$H+}
interface

uses
  Classes, SysUtils, istat.batch, istat.batch.firebird, ZDbcIntfs, paxutils;

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

  TPopolazioneProcessor = class(TStringAbstractItemProcessor<TIstatPopolazioneRecord>, IStringProcessor<TIstatPopolazioneRecord>)
    function process(const aIntput: string): TIstatPopolazioneRecord; override;
  end;

  { TPopolazioneDatabaseIstatDecessiWriter }

  TPopolazioneDatabaseIstatDecessiWriter = class(TFirebirdDatabaseWriter<TIstatPopolazioneRecord>)
  public
    procedure AfterConstruction; override;
    function Open: boolean; override;
    procedure Write(const item: TIstatPopolazioneRecord); override;
  end;

  { TChunkedPopolazioneDatabaseIstatDecessiWriter }

  TChunkedPopolazioneDatabaseIstatDecessiWriter = class(TFirebirdChunkedDatabaseWriter<TIstatPopolazioneRecord>)
  protected
    function statement(item: TIstatPopolazioneRecord): rawbytestring; override;
    function getTableName: string; override;
  public
  end;

  { TPopolazioneRunner }

  TPopolazioneRunner = class(TFirebirdStepRunner<TIstatPopolazioneRecord>)
  protected
    function getReaderDecessi: IStringReader; override;
    function getWriterDecessi: IItemWriter<TIstatPopolazioneRecord>; override;
    function getProcessor: IStringProcessor<TIstatPopolazioneRecord>; override;
  end;

implementation

{ TChunkedPopolazioneDatabaseIstatDecessiWriter }

function TChunkedPopolazioneDatabaseIstatDecessiWriter.statement(item: TIstatPopolazioneRecord): rawbytestring;
begin
  with item do
  begin
    Result := Format('INSERT INTO ISTAT_POPOLAZIONE VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);', [QuotedStr(ITTER107), QuotedStr(Territorio), QuotedStr(TIPO_DATO15), QuotedStr(Tipo_di_indicatore_demografico), QuotedStr(SEXISTAT1), QuotedStr(Sesso), QuotedStr(ETA1), QuotedStr(Eta), QuotedStr(STATCIV2), QuotedStr(Stato_civile), QuotedStr(TIME), QuotedStr(Seleziona_periodo), QuotedStr(Value), QuotedStr(Flag_Codes), QuotedStr(Flags)]);
  end;
end;

function TChunkedPopolazioneDatabaseIstatDecessiWriter.getTableName: string;
begin
  Result := 'ISTAT_POPOLAZIONE';
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

function TPopolazioneRunner.getProcessor: IStringProcessor<TIstatPopolazioneRecord>;
begin
  Result := TPopolazioneProcessor.Create;
end;

{ TPopolazioneDatabaseIstatDecessiWriter }

procedure TPopolazioneDatabaseIstatDecessiWriter.AfterConstruction;
begin
  inherited AfterConstruction;
  FTableName := 'ISTAT_POPOLAZIONE';
end;

function TPopolazioneDatabaseIstatDecessiWriter.Open: boolean;
begin
  FStatement := FConnection.PrepareStatement('INSERT INTO ISTAT_POPOLAZIONE VALUES(?,?,?,?,?, ?,?,?,?,?, ?,?,?,?,?)');
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

{ TPopolazioneProcessor }

function TPopolazioneProcessor.process(const aIntput: string): TIstatPopolazioneRecord;
var
  cursor: PChar;
begin
  cursor := PChar(aIntput);
  with Result do
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
