Диаграмма состояния после выделения `biddingService`
```plantuml
@startuml
!include https://raw.githubusercontent.com/plantuml-stdlib/C4-PlantUML/master/C4_Container.puml

LAYOUT_WITH_LEGEND()
LAYOUT_LEFT_RIGHT()

title C4 Container Diagram — AdScale Intermediate Architecture with API Gateway and Reliability Patterns

Person(advertiser, "Рекламодатель", "Управляет кампаниями, бюджетами и ставками через личный кабинет")
Person_Ext(endUser, "Пользователь сайта/приложения", "Просматривает рекламу, кликает по баннерам")

System_Ext(dspPartnerOld, "Текущие DSP/Ad Exchanges", "Bid requests / responses")
System_Ext(dspPartnerNew, "Новый DSP-партнёр", "Bid requests / responses по OpenRTB")
System_Ext(paymentGateway, "Платёжный шлюз", "Обработка пополнения бюджетов")

System_Boundary(edgeLayer, "Edge / API Layer") {
    Container(apiGateway, "API Gateway", "Envoy / Nginx", "TLS termination, routing, DSP auth, rate limiting, timeout budget, circuit breaker, metrics")
}

System_Boundary(monolith, "AdScale Monolith") {

    Container(adServer, "Ad Server", "C++ / Go", "Принимает bid requests, определяет пользователя, подбирает кандидатов")

    Container(auctionEngine, "Auction Engine", "C++", "Оркестрация аукциона, вызов biddingService, выбор победителя, fallback на legacy bidding")

    Container(deliveryService, "Delivery Service", "Node.js", "Формирует HTML/JS/OpenRTB response")

    Container(statsService, "Statistics Service", "Python", "Сбор кликов и показов. На промежуточном этапе запись переводится в очередь/буфер")

    Container(financialService, "Financial Service", "Go", "Финансы, балансы, списания, счета. Идемпотентные операции")

    Container(analyticsService, "Analytics Service", "Python", "Отчёты и аналитика. Ограничивается доступ к production-БД")

    Container(adCabinet, "Advertiser Dashboard", "Node.js + JavaScript", "UI управления кампаниями, ставками и отчётами")
}

System_Boundary(biddingBoundary, "New Extracted Service") {
    Container(biddingService, "biddingService", "Go / C++", "Расчёт effective bid, bid modifiers, стратегии ставок, floor price, pacing-aware adjustment, fallback на stale cache/default bid")
}

System_Boundary(dataLayer, "Data Layer") {
    ContainerDb(postgres, "PostgreSQL Primary", "PostgreSQL", "Текущая основная БД монолита")
    ContainerDb(readReplica, "PostgreSQL Read Replica", "PostgreSQL", "Реплика для аналитики и чтений вне hot path")
    ContainerDb(biddingDb, "Bidding DB", "PostgreSQL", "Конфигурация biddingService: стратегии, modifiers, версии правил")
    Container(servingCache, "Bidding Serving Cache", "Redis Cluster", "Горячие данные ставок: base bids, modifiers, strategies, floor rules")
}

System_Boundary(asyncLayer, "Async / Buffering Layer") {
    Container(eventQueue, "Event Queue", "Kafka / Redpanda", "Буферизация событий показов, кликов, bid events, campaign changes")
    Container(cacheSyncWorker, "Bidding Cache Sync Worker", "Go", "Загружает изменения ставок/кампаний из PostgreSQL/Bidding DB в Redis")
}

Rel(dspPartnerOld, apiGateway, "Bid requests / responses", "HTTPS / OpenRTB")
Rel(dspPartnerNew, apiGateway, "Bid requests / responses", "HTTPS / OpenRTB")
Rel(apiGateway, adServer, "Routes /openrtb/* bid requests", "HTTP / OpenRTB")

Rel(endUser, apiGateway, "Impressions / clicks", "HTTPS")
Rel(apiGateway, deliveryService, "Routes tracking/rendering requests", "HTTP")

Rel(advertiser, apiGateway, "Управляет кампаниями", "HTTPS")
Rel(apiGateway, adCabinet, "Routes dashboard/API traffic", "HTTPS / REST")

Rel(adCabinet, paymentGateway, "Пополнение бюджета", "HTTPS / REST")
Rel(paymentGateway, financialService, "Payment callbacks", "HTTPS")

Rel(adServer, auctionEngine, "Передаёт кандидатов", "Internal call")

Rel(auctionEngine, biddingService, "Запрашивает effective bids для кандидатов", "gRPC + timeout + circuit breaker")

Rel(auctionEngine, deliveryService, "Передаёт winner", "Internal call")

Rel(deliveryService, statsService, "Регистрирует показы/клики", "Internal call")
Rel(statsService, eventQueue, "Публикует impression/click events", "Async Kafka")
Rel(eventQueue, postgres, "Пакетная запись событий / consumers", "Async batch")

Rel(adCabinet, financialService, "Финансы", "Internal call / REST")
Rel(adCabinet, analyticsService, "Отчёты", "Internal call")

Rel(adServer, postgres, "Legacy reads для таргетинга", "SQL")
Rel(auctionEngine, postgres, "Минимальные legacy reads, постепенно сокращаются", "SQL")
Rel(financialService, postgres, "Идемпотентные финансовые транзакции", "SQL")
Rel(adCabinet, postgres, "CRUD кампаний и ставок", "SQL")

Rel(analyticsService, readReplica, "Тяжёлые read-запросы", "SQL")
Rel(readReplica, postgres, "Streaming replication", "PostgreSQL replication")

Rel(biddingService, servingCache, "Hot path read: base bids, modifiers, strategies", "Redis + timeout + stale fallback")
Rel(biddingService, biddingDb, "Admin/config path only, не в hot path", "SQL")
Rel(biddingService, eventQueue, "Публикует bid_calculated/no_bid events", "Async Kafka")

Rel(cacheSyncWorker, postgres, "Читает campaign/base bid changes", "SQL / polling or CDC-lite")
Rel(cacheSyncWorker, biddingDb, "Читает bidding strategies/modifiers", "SQL")
Rel(cacheSyncWorker, servingCache, "Обновляет serving snapshots", "Redis")
Rel(cacheSyncWorker, eventQueue, "Публикует cache_refresh events", "Async Kafka")

note right of apiGateway
  **API Gateway для DSP**
  - TLS termination
  - OpenRTB routing
  - DSP authentication
  - rate limiting per partner
  - timeout budget enforcement
  - circuit breaker / outlier detection
  - access logs
  - latency metrics P50/P95/P99
end note

note right of biddingService
  **Reliability**
  - no PostgreSQL in hot path
  - Redis timeout 2-5 ms
  - stale cache fallback
  - default bid fallback
  - partial response
  - publishes async bid events
end note

note right of financialService
  **Financial reliability**
  - idempotency key
  - unique transaction id
  - ledger-style writes
  - retry-safe API
  - reconciliation job
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

  Для Kafka:
  - at-least-once
  - idempotent consumers
  - DLQ
  - consumer lag monitoring
end note

legend right
  <#00AA00> Intermediate target in 3 months
  <#00AA00> Support at least 18 000 RPS
  <#00AA00> P95 auction latency ≤ 100 ms

  <#0088FF> New: API Gateway
  <#0088FF> New: extracted biddingService
  <#0088FF> New: Redis serving cache
  <#0088FF> New: Kafka/Event Queue
  <#0088FF> New: read replica

  <#FFAA00> Critical path:
  DSP → API Gateway → Ad Server → Auction Engine
  → biddingService → Auction Engine → Delivery → DSP

  <#FFAA00> Reliability:
  timeout, circuit breaker, fallback,
  idempotency, retries outside hot path
endlegend

@enduml
```
