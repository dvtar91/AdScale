Диаграмма состояния после выделения `biddingService`.

```plantuml
@startuml
!include https://raw.githubusercontent.com/plantuml-stdlib/C4-PlantUML/master/C4_Container.puml

LAYOUT_WITH_LEGEND()
LAYOUT_LEFT_RIGHT()

title C4 Container Diagram — AdScale Intermediate Architecture in 3 Months

Person(advertiser, "Рекламодатель", "Управляет кампаниями, бюджетами и ставками через личный кабинет")
Person_Ext(endUser, "Пользователь сайта/приложения", "Просматривает рекламу, кликает по баннерам")

System_Ext(dspPartnerOld, "Текущие DSP/Ad Exchanges", "Bid requests / responses")
System_Ext(dspPartnerNew, "Новый DSP-партнёр", "Bid requests / responses по OpenRTB")
System_Ext(paymentGateway, "Платёжный шлюз", "Обработка пополнения бюджетов")

System_Boundary(monolith, "AdScale Monolith") {

    Container(adServer, "Ad Server", "C++ / Go", "Принимает bid requests, определяет пользователя, подбирает кандидатов")

    Container(auctionEngine, "Auction Engine", "C++", "Оркестрация аукциона, вызов biddingService, выбор победителя")

    Container(deliveryService, "Delivery Service", "Node.js", "Формирует HTML/JS/OpenRTB response")

    Container(statsService, "Statistics Service", "Python", "Сбор кликов и показов. На промежуточном этапе запись переводится в очередь/буфер")

    Container(financialService, "Financial Service", "Go", "Финансы, балансы, списания, счета")

    Container(analyticsService, "Analytics Service", "Python", "Отчёты и аналитика. Ограничивается доступ к production-БД")

    Container(adCabinet, "Advertiser Dashboard", "Node.js + JavaScript", "UI управления кампаниями, ставками и отчётами")
}

System_Boundary(biddingBoundary, "New Extracted Service") {
    Container(biddingService, "biddingService", "Go / C++", "Расчёт effective bid, bid modifiers, стратегии ставок, floor price, pacing-aware adjustment")
}

System_Boundary(dataLayer, "Data Layer") {
    ContainerDb(postgres, "PostgreSQL Primary", "PostgreSQL", "Текущая основная БД монолита")
    ContainerDb(readReplica, "PostgreSQL Read Replica", "PostgreSQL", "Реплика для аналитики и чтений вне hot path")
    ContainerDb(biddingDb, "Bidding DB", "PostgreSQL", "Конфигурация biddingService: стратегии, modifiers, версии правил")
    Container(servingCache, "Bidding Serving Cache", "Redis Cluster", "Горячие данные ставок: base bids, modifiers, strategies, floor rules")
}

System_Boundary(asyncLayer, "Async / Buffering Layer") {
    Container(eventQueue, "Event Queue", "Kafka / Redpanda / RabbitMQ", "Буферизация событий показов, кликов, bid events, campaign changes") 
    Container(cacheSyncWorker, "Bidding Cache Sync Worker", "Go", "Загружает изменения ставок/кампаний из PostgreSQL/Bidding DB в Redis")
}

Rel(dspPartnerOld, adServer, "Bid requests / responses", "HTTPS / OpenRTB")
Rel(dspPartnerNew, adServer, "Bid requests / responses", "HTTPS / OpenRTB")
Rel(endUser, deliveryService, "Impressions / clicks", "HTTPS")
Rel(advertiser, adCabinet, "Управляет кампаниями", "HTTPS")
Rel(adCabinet, paymentGateway, "Пополнение бюджета", "HTTPS / REST")

Rel(adServer, auctionEngine, "Передаёт кандидатов", "Internal call")
Rel(auctionEngine, biddingService, "Запрашивает effective bids для кандидатов", "gRPC / HTTP")
Rel(auctionEngine, deliveryService, "Передаёт winner", "Internal call")

Rel(deliveryService, statsService, "Регистрирует показы/клики", "Internal call")
Rel(statsService, eventQueue, "Публикует impression/click events", "Async")
Rel(eventQueue, postgres, "Пакетная запись событий / consumers", "Async batch")

Rel(adCabinet, financialService, "Финансы", "Internal call")
Rel(adCabinet, analyticsService, "Отчёты", "Internal call")

Rel(adServer, postgres, "Legacy reads для таргетинга", "SQL")
Rel(auctionEngine, postgres, "Минимальные legacy reads, постепенно сокращаются", "SQL")
Rel(financialService, postgres, "Финансовые транзакции", "SQL")
Rel(adCabinet, postgres, "CRUD кампаний и ставок", "SQL")

Rel(analyticsService, readReplica, "Тяжёлые read-запросы", "SQL")
Rel(readReplica, postgres, "Streaming replication", "PostgreSQL replication")

Rel(biddingService, servingCache, "Hot path read: base bids, modifiers, strategies", "Redis")
Rel(biddingService, biddingDb, "Admin/config path only, не в hot path", "SQL")
Rel(biddingService, eventQueue, "Публикует bid_calculated/no_bid events", "Async")

Rel(cacheSyncWorker, postgres, "Читает campaign/base bid changes", "SQL / polling or CDC-lite")
Rel(cacheSyncWorker, biddingDb, "Читает bidding strategies/modifiers", "SQL")
Rel(cacheSyncWorker, servingCache, "Обновляет serving snapshots", "Redis")
Rel(cacheSyncWorker, eventQueue, "Публикует cache_refresh events", "Async")

Rel(paymentGateway, financialService, "Payment callbacks", "HTTPS")

note right of biddingService
  Новая граница:
  - расчёт effective bid
  - bid modifiers
  - strategy rules
  - floor price
  - pacing-aware adjustment

  Не выбирает winner.
  Не списывает деньги.
  Не читает PostgreSQL в hot path.
end note

note right of servingCache
  Главный источник данных biddingService
  в RTB hot path.

  Требования:
  - p95 read latency 1-3 ms
  - preloaded active campaigns
  - fallback на stale cache
  - TTL/versioning
end note

note bottom of eventQueue
  Очередь нужна, чтобы разделить:
  - critical RTB path
  - non-critical statistics/events path

  Это снижает write pressure на PostgreSQL.
end note

note right of readReplica
  Аналитика уводится с primary PostgreSQL,
  чтобы тяжёлые запросы не блокировали
  auction/bidding path.
end note

legend right
  <#00AA00> Intermediate target in 3 months
  <#00AA00> Support at least 18 000 RPS
  <#00AA00> P95 auction latency ≤ 100 ms
  <#00AA00> biddingService horizontally scalable

  <#0088FF> New: extracted biddingService
  <#0088FF> New: Redis serving cache
  <#0088FF> New: event queue for write buffering
  <#0088FF> New: read replica for analytics

  <#FFAA00> Critical path:
  DSP → Ad Server → Auction Engine → biddingService
  → Auction Engine → Delivery Service → DSP

  <#888888> Legacy monolith remains
endlegend

@enduml
```
