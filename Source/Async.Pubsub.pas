unit Async.Pubsub;

interface

uses
  System.Classes,
  System.Generics.Collections,
  System.SysUtils,
  System.Variants;

type
  TPubsubNotificationEvent = reference to procedure;
  TPubsubNotificationEventContent = reference to procedure (Content: Variant);

  TPubsubSubscribeParam = (pspNotifyFirst, pspNotifyLast);
  TPubsubSubscribeParams = set of TPubsubSubscribeParam;

  TPubsubSubscriptionId = type string;

  ISubscription = interface
  ['{DE5FA768-B9FF-4B24-A341-ED273A6F25D8}']
    function GetEvent: TPubsubNotificationEvent;
    function GetEventContent: TPubsubNotificationEventContent;
    function GetId: String;
    function GetParams: TPubsubSubscribeParams;
    property Id: String read GetId;
    property Event: TPubsubNotificationEvent read GetEvent;
    property EventContent: TPubsubNotificationEventContent read GetEventContent;
    property Params: TPubsubSubscribeParams read GetParams;
  end;

  Pubsub = class
  private
    class var
      Container: TDictionary<String, TInterfaceList>;
  private
    type
      ISubscriberNotification = interface
      ['{B84FFD07-D682-4BF8-B609-376B8134119D}']
        function NotifyOn(const Event: TPubsubNotificationEvent): TPubsubSubscriptionId; overload;
        function NotifyOn(const Event: TPubsubNotificationEvent; const Params: TPubsubSubscribeParams): TPubsubSubscriptionId; overload;
        function NotifyOn(const Event: TPubsubNotificationEventContent): TPubsubSubscriptionId; overload;
        function NotifyOn(const Event: TPubsubNotificationEventContent; const Params: TPubsubSubscribeParams): TPubsubSubscriptionId; overload;
      end;

      ISubscriber = interface
      ['{E86D5FC5-C119-4B0A-A20C-07879CEE32C9}']
        function OnQueue(const Queue: String): ISubscriberNotification;
      end;

      TSubscriber = class(TInterfacedObject, ISubscriber, ISubscriberNotification)
      private
        function GenerateId: String;
      public
        FId: String;
        FQueue: String;
        FEvent: TPubsubNotificationEvent;
        FEventContent: TPubsubNotificationEventContent;
        FParams: TPubsubSubscribeParams;
        function NotifyOn(const Event: TPubsubNotificationEvent): TPubsubSubscriptionId; overload;
        function NotifyOn(const Event: TPubsubNotificationEvent; const Params: TPubsubSubscribeParams): TPubsubSubscriptionId; overload;
        function NotifyOn(const Event: TPubsubNotificationEventContent): TPubsubSubscriptionId; overload;
        function NotifyOn(const Event: TPubsubNotificationEventContent; const Params: TPubsubSubscribeParams): TPubsubSubscriptionId; overload;
        function OnQueue(const Queue: String): ISubscriberNotification;
      end;

      TSubscription = class(TInterfacedObject, ISubscription)
      private
        FId: String;
        FEvent: TPubsubNotificationEvent;
        FEventContent: TPubsubNotificationEventContent;
        FParams: TPubsubSubscribeParams;
        function GetEvent: TPubsubNotificationEvent;
        function GetEventContent: TPubsubNotificationEventContent;
        function GetId: String;
        function GetParams: TPubsubSubscribeParams;
      public
        constructor Create(const Id: String; const Event: TPubsubNotificationEvent; const Params: TPubsubSubscribeParams); overload;
        constructor Create(const Id: String; const Event: TPubsubNotificationEventContent; const Params: TPubsubSubscribeParams); overload;
      end;

      IPublisher = interface
      ['{9772954F-F1ED-4EE9-A971-D0BB0A3833F9}']
        procedure OnQueue(const Queue: String);
      end;

      TPublisher = class(TInterfacedObject, IPublisher)
      private
        FContent: Variant;
        procedure OnQueue(const Queue: String);
      public
        constructor Create(const Content: Variant); reintroduce;
      end;
  private
    class procedure Init;
    class procedure Publish(const Queue: String; const Content: Variant); overload;
    class procedure Subscribe(const Queue: String; const Subscription: ISubscription); overload;
    class procedure Terminate;
  public
    class function Publish: IPublisher; overload;
    class function Publish(const Content: Variant): IPublisher; overload;
    class function Subscribe: ISubscriber; overload;
    class procedure Unsubscribe(const Id: TPubsubSubscriptionId);
  end;

  EPubsubInvalidQueueException = class(Exception)
    constructor Create; reintroduce;
  end;

implementation

uses
  System.Threading;

type
  TPublishTask = class(TTask, ITask)
  private
    FContent: Variant;
    FSubscribers: IInterfaceList;
    procedure Handle;
    constructor Create(const Subscribers: IInterfaceList; const Content: Variant); reintroduce;
  public
    class procedure Run(const Subscribers: IInterfaceList; const Content: Variant);
  end;

  TNotificationTask = class(TTask, ITask)
  private
    FContent: Variant;
    FSubscription: ISubscription;
    procedure Handle;
    constructor Create(const Subscription: ISubscription; const Content: Variant); reintroduce;
  public
    class procedure Run(const Subscription: ISubscription; const Content: Variant);
  end;

{ Pubsub }

class procedure Pubsub.Init;
begin
  Container := TDictionary<String, TInterfaceList>.Create;
end;

class procedure Pubsub.Subscribe(const Queue: String; const Subscription: ISubscription);
var
  Subscribers: TInterfaceList;
begin
  if not Container.TryGetValue(Queue, Subscribers) then
  begin
    Subscribers := TInterfaceList.Create;

    TMonitor.Enter(Container);
    try
      Container.Add(Queue, Subscribers);
    finally
      TMonitor.Exit(Container);
    end;
  end;

  if Subscription <> nil then
    Subscribers.Add(Subscription);
end;

class procedure Pubsub.Publish(const Queue: String;
  const Content: Variant);
var
  Subscribers: TInterfaceList;
begin
  if not Container.TryGetValue(Queue, Subscribers) then
    Exit;

  if Assigned(Subscribers) and (Subscribers.Count > 0) then
    TPublishTask.Run(Subscribers, Content);
end;

class function Pubsub.Publish: IPublisher;
begin
  Result := TPublisher.Create(null);
end;

class function Pubsub.Subscribe: ISubscriber;
begin
  Result := TSubscriber.Create;
end;

class procedure Pubsub.Terminate;
var
  O: TInterfaceList;
begin
  for O in Container.Values do
    if O <> nil then
    begin
      try
        O.Clear;
      except end;
      try
        O.Free;
      except end;
    end;

  try
    Container.Clear;
  except end;

  try
    FreeAndNil(Container);
  except end;
end;

class procedure Pubsub.Unsubscribe(const Id: TPubsubSubscriptionId);
begin
  TTask.Run(
    procedure
    var
      Found: Boolean;
      Index: Integer;
      Subscribers: TInterfaceList;
      Subscription: ISubscription;
    begin
      Found := False;
      
      for Subscribers in Container.Values do
      begin
        for Index := 0 to Pred(Subscribers.Count) do
        begin
          Subscription := Subscribers.Items[Index] as ISubscription;

          if (Subscription <> nil) and (Subscription.Id = Id) then
          begin
            Found := True;
            Break;
          end;
        end;

        if Found then
        begin
          TThread.Synchronize(nil,
            procedure
            begin
              TMonitor.Enter(Container);
              try
                Subscribers.Remove(Subscription);
              finally
                TMonitor.Exit(Container);
              end;
            end);
          Break;
        end;
      end;
    end);
end;

class function Pubsub.Publish(const Content: Variant): IPublisher;
begin
  Result := TPublisher.Create(Content);
end;

{ Pubsub.TSubscriber }

function Pubsub.TSubscriber.NotifyOn(const Event: TPubsubNotificationEvent;
  const Params: TPubsubSubscribeParams): TPubsubSubscriptionId;
begin
  FEvent := Event;
  FParams := Params;
  FId := GenerateId;
  Result := FId;

  Pubsub.Subscribe(FQueue, TSubscription.Create(FId, FEvent, FParams));
end;

function Pubsub.TSubscriber.GenerateId: String;
begin
  Result := Random(999999999).ToString
end;

function Pubsub.TSubscriber.NotifyOn(const Event: TPubsubNotificationEventContent;
  const Params: TPubsubSubscribeParams): TPubsubSubscriptionId;
begin
  FEventContent := Event;
  FParams := Params;
  FId := GenerateId;
  Result := FId;

  Pubsub.Subscribe(FQueue, TSubscription.Create(FId, FEvent, FParams));
end;

function Pubsub.TSubscriber.NotifyOn(
  const Event: TPubsubNotificationEventContent): TPubsubSubscriptionId;
begin
  Result := NotifyOn(Event, []);
end;

function Pubsub.TSubscriber.OnQueue(const Queue: String): ISubscriberNotification;
begin
  if Queue.Trim = EmptyStr then
    EPubsubInvalidQueueException.Create;

  FQueue := Queue;
  Result := Self;
end;

function Pubsub.TSubscriber.NotifyOn(const Event: TPubsubNotificationEvent): TPubsubSubscriptionId;
begin
  Result := NotifyOn(Event, []);
end;

{ Pubsub.TSubscription }

constructor Pubsub.TSubscription.Create(const Id: String;
  const Event: TPubsubNotificationEventContent;
  const Params: TPubsubSubscribeParams);
begin
  FId := Id;
  FEventContent := Event;
  FParams := Params;
end;

constructor Pubsub.TSubscription.Create(const Id: String;
  const Event: TPubsubNotificationEvent; const Params: TPubsubSubscribeParams);
begin
  FId := Id;
  FEvent := Event;
  FParams := Params;
end;

function Pubsub.TSubscription.GetEvent: TPubsubNotificationEvent;
begin
  Result := FEvent;
end;

function Pubsub.TSubscription.GetEventContent: TPubsubNotificationEventContent;
begin
  Result := FEventContent;
end;

function Pubsub.TSubscription.GetId: String;
begin
  Result := FId;
end;

function Pubsub.TSubscription.GetParams: TPubsubSubscribeParams;
begin
  Result := FParams;
end;

{ Pubsub.TPublisher }

constructor Pubsub.TPublisher.Create(const Content: Variant);
begin
  FContent := Content;
end;

procedure Pubsub.TPublisher.OnQueue(const Queue: String);
begin
  if Queue.Trim = EmptyStr then
    raise EPubsubInvalidQueueException.Create;

  Pubsub.Publish(Queue, FContent);
end;

{ EPubsubInvalidQueueException }

constructor EPubsubInvalidQueueException.Create;
begin
  inherited Create('Invalid queue name was provided.');
end;

{ TPublishTask }

constructor TPublishTask.Create(const Subscribers: IInterfaceList; const Content: Variant);
begin
  FContent := Content;
  FSubscribers := Subscribers;

  inherited Create(nil, TNotifyEvent(nil),
    procedure
    begin
      Handle;
    end, TThreadPool.Default, nil, []);
end;

procedure TPublishTask.Handle;
var
  Index: Integer;
  OnTerminate: IInterfaceList;
  Subscription: ISubscription;
begin
  OnTerminate := TInterfaceList.Create;
  try
    for Index := 0 to Pred(FSubscribers.Count) do
    begin
      Subscription := FSubscribers.Items[Index] as ISubscription;

      if not Assigned(Subscription) then
        Continue;

      if pspNotifyLast in Subscription.Params then
        OnTerminate.Add(Subscription)
      else
        TNotificationTask.Run(Subscription, FContent);
    end;

    for Index := 0 to Pred(OnTerminate.Count) do
      TNotificationTask.Run(OnTerminate.Items[Index] as ISubscription, FContent);
  finally
    OnTerminate.Clear;
    OnTerminate := nil;
  end;
end;

class procedure TPublishTask.Run(const Subscribers: IInterfaceList; const Content: Variant);
var
  Task: ITask;
begin
  Task := TPublishTask.Create(Subscribers, Content);
  Task.Start;
end;

{ TNotificationTask }

constructor TNotificationTask.Create(const Subscription: ISubscription;
  const Content: Variant);
begin
  FContent := Content;
  FSubscription := Subscription;

  inherited Create(nil, TNotifyEvent(nil),
    procedure
    begin
      Handle;
    end, TThreadPool.Default, nil, []);
end;

procedure TNotificationTask.Handle;
begin

end;

class procedure TNotificationTask.Run(const Subscription: ISubscription;
  const Content: Variant);
var
  Task: ITask;
begin
  Task := TNotificationTask.Create(Subscription, Content);
  Task.Start;
end;

initialization
  Pubsub.Init;

finalization
  Pubsub.Terminate;

end.
