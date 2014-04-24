{

TRedisWire: Redis.pas

Copyright 2014 Stijn Sanders
Made available under terms described in file "LICENSE"
https://github.com/stijnsanders/TRedisWire

}
unit Redis;

interface

uses SysUtils, Sockets;

type
  TRedisWire=class(TObject)
  private
    FSocket:TTcpClient;
    FTimeoutMS:cardinal;
    procedure SetTimeoutMS(const Value: cardinal);
  public
    constructor Create(const Host:string;Port:integer=6379);
    destructor Destroy; override;

    function Cmd(const Cmd:string):OleVariant; overload;
    function Cmd(const Args:array of OleVariant):OleVariant; overload;

    function Get_(const Key:string):OleVariant;
    procedure Set_(const Key:string; Value:OleVariant);

    property TimeoutMS:cardinal read FTimeoutMS write SetTimeoutMS;
    property Values[const Key:string]:OleVariant read Get_ write Set_; default;
  end;

  ERedisError=class(Exception);

implementation

uses Variants, WinSock, Classes;

{ TRedisWire }

constructor TRedisWire.Create(const Host: string; Port: integer);
begin
  inherited Create;
  FSocket:=TTcpClient.Create(nil);
  //FSocket.BlockMode:=bmBlocking;
  FSocket.RemoteHost:=Host;
  FSocket.RemotePort:=IntToStr(Port);
  //FSocket.Open;//see check
  FTimeoutMS:=10000;//default
end;

destructor TRedisWire.Destroy;
begin
  FSocket.Free;
  inherited;
end;

function TRedisWire.Cmd(const Cmd: string): OleVariant;
var
  Data:AnsiString;
  DataLength,DataIndex,DataLast,DataNext,ArrayLength,ArrayIndex:integer;
  InArray:boolean;
  function ReadInt:integer;
  var
    i:integer;
  begin
    Result:=0;
    inc(DataIndex);
    while (DataIndex<=DataLength) and (Data[DataIndex]<>#13) do
     begin
      while (DataIndex<=DataLength) and (Data[DataIndex]<>#13) do
       begin
        Result:=Result*10+(byte(Data[DataIndex]) and $0F);
        inc(DataIndex);
       end;
      if (DataIndex>DataLength) then
       begin
        if DataLast<>1 then
         begin
          DataLength:=DataLength-DataLast+1;
          Data:=Copy(Data,DataLast,DataLength);
          DataLast:=1;
         end;
        SetLength(Data,DataLength+$10000);
        i:=FSocket.ReceiveBuf(Data[DataLength+1],$10000);
        if (i=-1) then
          raise ERedisError.Create(SysErrorMessage(WSAGetLastError));
        inc(DataLength,i);
       end;
     end;
    inc(DataIndex,2);//#13#10
  end;
var
  i:integer;  
begin
  //check connection
  if not FSocket.Connected then
   begin
    FSocket.Connect;
    //TODO: failover server list?
    DataIndex:=1;
    setsockopt(FSocket.Handle,IPPROTO_TCP,TCP_NODELAY,@DataIndex,4);
    setsockopt(FSocket.Handle,SOL_SOCKET,SO_RCVTIMEO,PAnsiChar(@FTimeoutMS),4);
   end;
  //send command
  Data:=Cmd;//UTF8Encode?
  DataLength:=Length(Data);
  //TODO: check Copy(Data,DataLength-1,2)=#13#10?
  if FSocket.SendBuf(Data[1],DataLength)<>DataLength then
    raise ERedisError.Create(SysErrorMessage(WSAGetLastError));
  //get a first block
  SetLength(Data,$10000);
  DataLength:=FSocket.ReceiveBuf(Data[1],$10000);
  if (DataLength=-1) or (DataLength=0) then
    raise ERedisError.Create(SysErrorMessage(WSAGetLastError));
  //SetLength(Data,DataLength);
  InArray:=false;
  ArrayIndex:=0;
  DataIndex:=1;
  DataLast:=1;
  while (DataIndex<=DataLength) do
   begin
    DataLast:=DataIndex;
    case Data[DataIndex] of
      '-'://error
        raise ERedisError.Create(
          Copy(Data,DataIndex+1,DataLength-DataIndex-2));
      '+'://message
       begin
        if InArray then raise ERedisError.Create('Unexpected message in array');
        Result:=Data='+OK'#13#10;//boolean?
        DataIndex:=DataLength+1;
       end;
      '*'://array
       begin
        if InArray then raise ERedisError.Create('Unexpected nested array');
        InArray:=true;
        ArrayIndex:=0;
        ArrayLength:=ReadInt;
        //TODO: detect array type?
        Result:=VarArrayCreate([0,ArrayLength-1],varVariant);
       end;
      ':'://integer
        if InArray then
         begin
          Result[ArrayIndex]:=ReadInt;
          inc(ArrayIndex);
         end
        else
          Result:=ReadInt;
      '$'://dump string
        if (DataIndex+3<DataLength) and (Data[DataIndex+1]='-') and
          (Data[DataIndex+2]='1') then
         begin
          if InArray then
           begin
            Result[ArrayIndex]:=Null;
            inc(ArrayIndex);
           end
          else
            Result:=Null;
          inc(DataIndex,5);
         end
        else
         begin
          DataNext:=ReadInt;
          if (DataLast<>1) and (DataIndex+DataNext>DataLength) then
           begin
            DataLength:=DataLength-DataLast+1;
            Data:=Copy(Data,DataLast,DataLength);
            DataLast:=1;
           end;
          while DataIndex+DataNext>DataLength do
           begin
            SetLength(Data,DataLength+$10000);
            i:=FSocket.ReceiveBuf(Data[DataLength+1],$10000);
            if (i=-1) then
              raise ERedisError.Create(SysErrorMessage(WSAGetLastError));
            inc(DataLength,i);
           end;
          //TODO: variant type convertors?
          if InArray then
           begin
            Result[ArrayIndex]:=Copy(Data,DataIndex,DataLength);
            inc(ArrayIndex);
           end
          else
            Result:=Copy(Data,DataIndex,DataLength);
          inc(DataIndex,DataLength);
          inc(DataIndex,2);//#13#10
         end;
      else raise ERedisError.Create('Unknown response type: '+Data);
    end;
    //if InArray and (ArrayIndex<ArrayLength)...
   end;
end;

procedure TRedisWire.SetTimeoutMS(const Value: cardinal);
begin
  FTimeoutMS:=Value;
  if FSocket.Connected then
    setsockopt(FSocket.Handle,SOL_SOCKET,SO_RCVTIMEO,PAnsiChar(@FTimeoutMS),4);
end;

function TRedisWire.Get_(const Key: string): OleVariant;
begin
  Result:=Cmd('GET '+Key+#13#10);
end;

procedure TRedisWire.Set_(const Key: string; Value: OleVariant);
begin
  //TODO: encode value
  Cmd('SET '+Key+' "'+VarToStr(Value)+'"'#13#10);
end;

function TRedisWire.Cmd(const Args: array of OleVariant): OleVariant;
var
  s,t:string;
  i:integer;
  vt:word;
begin
  s:='*'+IntToStr(Length(Args))+#13#10;
  for i:=0 to Length(Args)-1 do
   begin
    vt:=VarType(Args[i]);
    if (vt and varArray)<>0 then
      raise ERedisError.Create('#'+IntToStr(i)+': Nested arrays not supported');
    case vt and varTypeMask of
      varEmpty,varNull:
        s:=s+'$-1'#13#10;
      varSmallint,varInteger,
      varShortInt,varByte,varWord,varLongWord,varInt64:
        s:=s+':'+VarToStr(Args[i])+#13#10;
      else
       begin
        t:=VarToStr(Args[i]);
        s:=s+'$'+IntToStr(Length(t))+#13#10+t+#13#10;
       end;
      //else raise ERedisError.Create('#'+IntToStr(i)+': Variant type not supported');
    end;
   end;
  Result:=Cmd(s);
end;

end.
