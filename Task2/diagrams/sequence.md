## sequence-диаграммы

1. RTB / Bidding hot path  
2. Campaign management / изменение ставки  
3. Clicks & impressions / потоковая обработка событий через Kafka  

---

# 1. RTB / Bidding hot path

```plantuml
@startuml
title Sequence Diagram — RTB / Bidding Hot Path

autonumber

actor "DSP Partner" as DSP
participant "API Gateway\nNginx / Envoy" as Gateway
participant "Ad Server\nMonolith" as AdServer
participant "Auction Engine\nMonolith" as Auction
participant "biddingService" as Bidding
database "Redis\nBidding Serving Cache" as Redis
participant "Delivery Service\nMonolith" as Delivery
queue "Kafka / Event Queue" as Kafka

== Bid request ==

DSP -> Gateway: POST /openrtb/2.5/bid\nHTTPS + OpenRTB JSON
activate Gateway

Gateway -> Gateway: TLS termination\nrate limit\npartner auth\nrequest timeout budget

Gateway -> AdServer: Forward bid request\nHTTP internal
activate AdServer

AdServer -> AdServer: Validate OpenRTB\nnormalize request\nidentify user/context

AdServer -> Auction: Pass candidates + auction context\nInternal call
activate Auction

Auction -> Auction: Build CalculateBidsRequest\nset deadline 10-20 ms

== Effective bid calculation ==

Auction -> Bidding: CalculateBids(request)\ngRPC / Protobuf
activate Bidding

Bidding -> Redis: GET bid configs/modifiers/floor rules\nRedis protocol
activate Redis
Redis --> Bidding: Config snapshot
deactivate Redis

Bidding -> Bidding: Calculate effective bids\napply modifiers/strategy/floor

Bidding -[#gray]> Kafka: publish bid_calculated / no_bid events\nAsync, non-blocking
note right of Kafka
  Событие не должно блокировать
  RTB hot path.
end note

Bidding --> Auction: CalculateBidsResponse\nOK / PARTIAL / TIMEOUT
deactivate Bidding

alt biddingService OK or PARTIAL
    Auction -> Auction: Select winner\nauction mechanics
else biddingService TIMEOUT/ERROR
    Auction -> Auction: Fallback to legacy bidding\nor no-bid if required
end

== Response building ==

Auction -> Delivery: Winner + response context\nInternal call
activate Delivery

Delivery -> Delivery: Build OpenRTB bid response\nor HTML/JS markup

Delivery -[#gray]> Kafka: publish auction_result / render metadata\nAsync
Delivery --> Auction: Response payload
deactivate Delivery

Auction --> AdServer: Auction result response
deactivate Auction

AdServer --> Gateway: OpenRTB bid response\nor no-bid
deactivate AdServer

Gateway --> DSP: HTTP 200 bid response\nor HTTP 204 no-bid
deactivate Gateway

@enduml
```

---

# 2. RTB / Bidding hot path с timeout и fallback

Эта диаграмма отдельно показывает критичный сценарий деградации `biddingService`.

```plantuml
@startuml
title Sequence Diagram — RTB Bidding Timeout and Fallback

autonumber

actor "DSP Partner" as DSP
participant "API Gateway" as Gateway
participant "Ad Server\nMonolith" as AdServer
participant "Auction Engine\nMonolith" as Auction
participant "biddingService" as Bidding
database "Redis\nServing Cache" as Redis
participant "Delivery Service" as Delivery
queue "Kafka" as Kafka

DSP -> Gateway: POST /openrtb/2.5/bid\nOpenRTB JSON
activate Gateway

Gateway -> Gateway: Apply partner timeout\nexample: 80 ms

Gateway -> AdServer: Forward request
activate AdServer

AdServer -> Auction: Candidates + context
activate Auction

Auction -> Bidding: CalculateBids(deadline=15ms)\ngRPC
activate Bidding

alt Redis slow or unavailable
    Bidding -> Redis: GET configs
    Redis --x Bidding: timeout/error

    Bidding -> Bidding: Use local stale snapshot\nor base_bid fallback
    Bidding --> Auction: PARTIAL response\nfallbackApplied=true
else biddingService slow
    Bidding --x Auction: deadline exceeded
    deactivate Bidding

    Auction -> Auction: Circuit breaker records failure
    Auction -> Auction: Use legacy embedded bidding
else biddingService unavailable
    Auction --x Bidding: connection error
    Auction -> Auction: Use legacy embedded bidding
end

Auction -> Auction: Select winner if possible

alt winner exists
    Auction -> Delivery: Build response
    activate Delivery
    Delivery --> Auction: Bid response
    deactivate Delivery

    Auction --> AdServer: Bid response
    AdServer --> Gateway: Bid response
    Gateway --> DSP: HTTP 200 OpenRTB response
else no eligible bid
    Auction --> AdServer: No-bid
    AdServer --> Gateway: No-bid
    Gateway --> DSP: HTTP 204 No Content
end

Auction -[#gray]> Kafka: publish bidding_timeout / fallback_used\nAsync

deactivate Auction
deactivate AdServer
deactivate Gateway

@enduml
```

---

# 3. Campaign management / изменение ставки

Поток для изменения ставки рекламодателем через кабинет.  
Основной API — REST. После изменения публикуется событие в Kafka, а `Cache Sync Worker` обновляет Redis для `biddingService`.

```plantuml
@startuml
title Sequence Diagram — Campaign Bid Change and Bidding Cache Update

autonumber

actor "Advertiser" as Advertiser
participant "Advertiser Web UI" as UI
participant "API Gateway\nNginx / Envoy" as Gateway
participant "Campaign API\nMonolith / Campaign module" as Campaign
database "PostgreSQL\nPrimary" as Postgres
database "Outbox Table" as Outbox
queue "Kafka" as Kafka
participant "Cache Sync Worker" as Worker
database "Redis\nBidding Serving Cache" as Redis

== Advertiser changes bid ==

Advertiser -> UI: Change base bid
UI -> Gateway: PATCH /api/v1/ad-groups/{id}/bid\nREST/JSON
activate Gateway

Gateway -> Gateway: AuthN/AuthZ\nrate limit\nrequest validation

Gateway -> Campaign: PATCH bid\nREST/JSON
activate Campaign

Campaign -> Campaign: Validate business rules\ncurrency, min/max bid, ownership

== Transactional update ==

Campaign -> Postgres: BEGIN
activate Postgres

Campaign -> Postgres: UPDATE ad_group\nSET base_bid = new value,\nversion = version + 1

Campaign -> Outbox: INSERT outbox event\nbase_bid_changed
activate Outbox
Outbox --> Campaign: inserted
deactivate Outbox

Campaign -> Postgres: COMMIT
deactivate Postgres

Campaign --> Gateway: 200 OK\nupdated bid + version
deactivate Campaign

Gateway --> UI: 200 OK
deactivate Gateway

UI --> Advertiser: Show success

== Async event publication ==

participant "Outbox Publisher" as Publisher

Publisher -> Outbox: Poll unpublished events
activate Outbox
Outbox --> Publisher: base_bid_changed event
deactivate Outbox

Publisher -> Kafka: Produce campaign.changed.v1\nkey=ad_group_id
activate Kafka
Kafka --> Publisher: ack
deactivate Kafka

Publisher -> Outbox: Mark event as published

== Serving cache update ==

Worker -> Kafka: Consume campaign.changed.v1
activate Kafka
Kafka --> Worker: base_bid_changed event
deactivate Kafka

Worker -> Postgres: Read latest campaign/ad_group state\nby id/version
activate Postgres
Postgres --> Worker: latest base bid/config
deactivate Postgres

Worker -> Redis: Update bidcfg:adgroup:{id}\nwith new snapshotVersion
activate Redis
Redis --> Worker: OK
deactivate Redis

Worker -> Kafka: Produce bidding.cache_refreshed.v1\noptional

@enduml
```

---

# 4. Campaign management без Outbox, упрощённый вариант

Если outbox не успевают внедрить за 3 месяца, можно сделать проще, но это менее надёжно.

```plantuml
@startuml
title Sequence Diagram — Campaign Bid Change Without Outbox, Simplified

autonumber

actor "Advertiser" as Advertiser
participant "Advertiser Web UI" as UI
participant "API Gateway" as Gateway
participant "Campaign API\nMonolith" as Campaign
database "PostgreSQL" as Postgres
queue "Kafka" as Kafka
participant "Cache Sync Worker" as Worker
database "Redis\nBidding Serving Cache" as Redis

Advertiser -> UI: Change bid

UI -> Gateway: PATCH /api/v1/ad-groups/{id}/bid
Gateway -> Campaign: Forward REST request

Campaign -> Postgres: UPDATE ad_group base_bid
activate Postgres
Postgres --> Campaign: OK
deactivate Postgres

Campaign -> Kafka: Produce base_bid_changed
activate Kafka
Kafka --> Campaign: ack
deactivate Kafka

Campaign --> Gateway: 200 OK
Gateway --> UI: 200 OK
UI --> Advertiser: Show success

Worker -> Kafka: Consume base_bid_changed
Kafka --> Worker: Event

Worker -> Redis: Update bid config
Redis --> Worker: OK

note right of Campaign
  Риск:
  PostgreSQL UPDATE успешен,
  но Kafka publish может упасть.

  Тогда Redis останется со старой ставкой.
  Поэтому outbox предпочтительнее.
end note

@enduml
```

---

# 5. Clicks & impressions через Kafka

Поток событий кликов и показов.  
Цель — убрать прямую синхронную запись в PostgreSQL из request path.

```plantuml
@startuml
title Sequence Diagram — Impression / Click Event Streaming via Kafka

autonumber

actor "End User" as User
participant "API Gateway\nNginx / Envoy" as Gateway
participant "Tracking / Statistics Adapter" as Tracking
queue "Kafka" as Kafka
participant "Stats Consumer" as StatsConsumer
participant "Billing Consumer" as BillingConsumer
participant "Analytics Consumer" as AnalyticsConsumer
database "PostgreSQL / Stats DB" as StatsDb
database "Finance DB / Monolith DB" as FinanceDb
database "Analytics Storage" as AnalyticsDb

== Impression or click received ==

User -> Gateway: GET /track/impression?... \nor GET /track/click?...
activate Gateway

Gateway -> Gateway: Rate limit\nbot/basic validation\nrequest id

Gateway -> Tracking: Forward tracking event\nHTTP
activate Tracking

Tracking -> Tracking: Validate event\ncreate eventId\nnormalize payload

Tracking -> Kafka: Produce impression_events / click_events\nkey=campaign_id or auction_id
activate Kafka
Kafka --> Tracking: ack
deactivate Kafka

Tracking --> Gateway: 204 No Content\nor tracking pixel
deactivate Tracking

Gateway --> User: 204 / 1x1 pixel
deactivate Gateway

== Async consumers ==

Kafka -> StatsConsumer: Consume events\nconsumer group: stats
activate StatsConsumer

StatsConsumer -> StatsConsumer: Aggregate / deduplicate\nbatch
StatsConsumer -> StatsDb: Batch insert / update aggregates
activate StatsDb
StatsDb --> StatsConsumer: OK
deactivate StatsDb
deactivate StatsConsumer

Kafka -> BillingConsumer: Consume billable events\nconsumer group: billing
activate BillingConsumer

BillingConsumer -> BillingConsumer: Deduplicate by eventId\ncalculate spend
BillingConsumer -> FinanceDb: Idempotent spend transaction
activate FinanceDb
FinanceDb --> BillingConsumer: OK
deactivate FinanceDb
deactivate BillingConsumer

Kafka -> AnalyticsConsumer: Consume raw events\nconsumer group: analytics
activate AnalyticsConsumer

AnalyticsConsumer -> AnalyticsDb: Write raw/enriched event
activate AnalyticsDb
AnalyticsDb --> AnalyticsConsumer: OK
deactivate AnalyticsConsumer

@enduml
```

---

# 6. Clicks & impressions с ошибкой consumer и retry/DLQ

```plantuml
@startuml
title Sequence Diagram — Kafka Consumer Retry and DLQ for Tracking Events

autonumber

participant "Tracking / Statistics Adapter" as Tracking
queue "Kafka\nimpression_events / click_events" as Kafka
participant "Stats Consumer" as Consumer
database "Stats DB" as StatsDb
queue "Kafka\nDLQ topic" as DLQ
participant "Ops / Support" as Ops

Tracking -> Kafka: Produce event\nkey=campaign_id
Kafka --> Tracking: ack

Kafka -> Consumer: Deliver event
activate Consumer

Consumer -> Consumer: Validate schema\ncheck eventId

alt valid event and DB available
    Consumer -> StatsDb: Batch insert
    activate StatsDb
    StatsDb --> Consumer: OK
    deactivate StatsDb

    Consumer -> Kafka: Commit offset
else temporary DB error
    Consumer -> StatsDb: Batch insert
    StatsDb --x Consumer: timeout/error

    Consumer -> Consumer: Retry with backoff

    alt retry succeeds
        Consumer -> StatsDb: Batch insert
        StatsDb --> Consumer: OK
        Consumer -> Kafka: Commit offset
    else retry exhausted
        Consumer -> DLQ: Produce failed event\nwith error reason
        Consumer -> Kafka: Commit offset
    end
else invalid schema or poison message
    Consumer -> DLQ: Produce invalid event\nvalidation_error
    Consumer -> Kafka: Commit offset
end

deactivate Consumer

Ops -> DLQ: Inspect failed events
DLQ --> Ops: Failed payload + error reason

@enduml
```

---

# 7. Полный промежуточный поток: изменение ставки влияет на RTB

Эта диаграмма показывает связь между Campaign REST API, Kafka, Redis cache и последующим использованием новой ставки в `biddingService`.

```plantuml
@startuml
title Sequence Diagram — Bid Change Propagation to RTB Hot Path

autonumber

actor "Advertiser" as Advertiser
participant "Advertiser Web UI" as UI
participant "API Gateway" as Gateway
participant "Campaign API\nMonolith" as Campaign
database "PostgreSQL" as Postgres
database "Outbox Table" as Outbox
participant "Outbox Publisher" as Publisher
queue "Kafka" as Kafka
participant "Cache Sync Worker" as Worker
database "Redis\nBidding Serving Cache" as Redis

actor "DSP Partner" as DSP
participant "Ad Server" as AdServer
participant "Auction Engine" as Auction
participant "biddingService" as Bidding

== Bid change ==

Advertiser -> UI: Set base bid = 1.50
UI -> Gateway: PATCH /api/v1/ad-groups/ag-1/bid
Gateway -> Campaign: REST PATCH bid

Campaign -> Postgres: BEGIN
Campaign -> Postgres: UPDATE ad_group\nbase_bid=1.50, version=43
Campaign -> Outbox: INSERT base_bid_changed\nversion=43
Campaign -> Postgres: COMMIT

Campaign --> Gateway: 200 OK version=43
Gateway --> UI: 200 OK
UI --> Advertiser: Bid updated

== Event publication ==

Publisher -> Outbox: Poll unpublished events
Outbox --> Publisher: base_bid_changed version=43

Publisher -> Kafka: Produce campaign.changed.v1\nkey=ag-1
Kafka --> Publisher: ack

Publisher -> Outbox: Mark published

== Cache refresh ==

Worker -> Kafka: Consume campaign.changed.v1
Kafka --> Worker: base_bid_changed ag-1 version=43

Worker -> Postgres: SELECT latest ad_group config\nWHERE ad_group_id='ag-1'
Postgres --> Worker: base_bid=1.50 version=43

Worker -> Redis: SET bidcfg:adgroup:ag-1\nbase_bid=1.50\nsnapshotVersion=34812
Redis --> Worker: OK

== Later RTB request uses new bid ==

DSP -> Gateway: POST /openrtb/2.5/bid
Gateway -> AdServer: Forward bid request

AdServer -> Auction: Candidates includes ag-1

Auction -> Bidding: CalculateBids(candidates=[ag-1])\ngRPC

Bidding -> Redis: GET bidcfg:adgroup:ag-1
Redis --> Bidding: base_bid=1.50\nsnapshotVersion=34812

Bidding -> Bidding: Apply modifiers/strategy/floor
Bidding --> Auction: effective_bid=1.72

Auction -> Auction: Select winner
Auction --> AdServer: Bid response
AdServer --> Gateway: OpenRTB response
Gateway --> DSP: HTTP 200 bid response

@enduml
```
