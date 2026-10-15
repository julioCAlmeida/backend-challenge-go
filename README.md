# Wallet Service — processamento distribuído de apostas em Go

Serviço em **Go 1.27 + Uber Fx** que movimenta carteiras de jogadores a partir de operações de provedores de jogos (`BET`, `WIN`, `LOSS`, `REFUND`, `ROLLBACK`), recebidas por **HTTP** e por **SQS FIFO**, com:

- dinheiro em `int64` de centavos (nunca `float`), ledger *append-only* imposto pelo banco;
- idempotência persistente no PostgreSQL, compartilhada por HTTP e SQS;
- coordenação por carteira (`SELECT … FOR UPDATE` + update condicionado por versão), sem lock global;
- inbox/outbox transacionais, retry com backoff, DLQ e retomada durável de pendências;
- autenticação OAuth 2.0 (`client_credentials`) com **Keycloak** e autorização por papel/provedor;
- três instâncias independentes no Docker Compose.

O enunciado original está em [CHALLENGE.md](CHALLENGE.md). As decisões técnicas estão em [ARCHITECTURE.md](ARCHITECTURE.md).

---

## 1. Pré-requisitos

| Ferramenta | Versão usada | Para quê |
| --- | --- | --- |
| Docker + Docker Compose v2 | 28.x / 2.40 | ambiente completo (PostgreSQL 17, Keycloak 26.3, LocalStack 4.4, 3 instâncias) |
| Go | 1.27.2 (mesma do `go.mod` e do `Dockerfile`) | testes, `go vet`, ferramentas locais |
| `curl` | qualquer | exemplos de chamadas |

Portas usadas no host: `8081-8083` (instâncias), `8180` (Keycloak), `55432` (PostgreSQL), `4566` (LocalStack). Podem ser alteradas via `.env` (veja `.env.example`).

## 2. Subindo o ambiente

```sh
cp .env.example .env        # opcional: os mesmos valores já são os defaults do compose
docker compose up --build   # ou -d para rodar em background
```

Ordem de inicialização (garantida por `depends_on` + healthchecks):

1. `postgres` — cria o papel de menor privilégio `wallet_app` (`deploy/postgres/init-roles.sh`);
2. `keycloak` — importa automaticamente o realm `wagering` com clientes e papéis de teste (`deploy/keycloak/wagering-realm.json`);
3. `localstack` — SQS;
4. `migrate` — aplica as migrations (`/app/migrate up`) com o dono do schema;
5. `sqs-provision` — cria as filas FIFO, DLQs, redrive e políticas de acesso;
6. `app-1`, `app-2`, `app-3` — três processos independentes do serviço (portas 8081, 8082, 8083).

Verificação:

```sh
curl -s localhost:8081/health/ready   # {"checks":{"postgres":"ok","sqs":"ok"},"status":"ready"}
```

Para encerrar: `docker compose down` (adicione `-v` para apagar o volume do banco).

## 3. Variáveis de ambiente do serviço

| Variável | Default | Descrição |
| --- | --- | --- |
| `INSTANCE_ID` | hostname | identificação da instância (logs, dono de leases da outbox) |
| `HTTP_ADDR` | `:8080` | endereço HTTP |
| `LOG_LEVEL` | `info` | `debug`, `info`, `warn`, `error` |
| `SHUTDOWN_TIMEOUT` / `START_TIMEOUT` | `25s` / `60s` | prazos do ciclo de vida Fx |
| `DATABASE_URL` | — (obrigatória) | URL do PostgreSQL (papel `wallet_app`) |
| `DATABASE_MAX_CONNS` | `20` | tamanho do pool |
| `DATABASE_STATEMENT_TIMEOUT` / `DATABASE_LOCK_TIMEOUT` | `10s` / `5s` | prazos por comando / espera de lock |
| `CONFLICT_RETRIES` | `5` | tentativas em deadlock/serialização/versão |
| `AWS_REGION`, `SQS_ENDPOINT` | `us-east-1`, — | região e endpoint (LocalStack/MiniStack) |
| `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY` | — | credenciais do serviço no broker |
| `SQS_WAGER_QUEUE` / `SQS_WAGER_DLQ` / `SQS_EVENTS_QUEUE` | `wager-transactions.fifo` / `wager-transactions-dlq.fifo` / `wallet-events.fifo` | filas |
| `SQS_CONSUMER_ENABLED`, `SQS_CONSUMER_WORKERS` | `true`, `2` | consumidor |
| `SQS_WAIT_TIME` | `10s` | long polling |
| `SQS_VISIBILITY_TIMEOUT` / `SQS_PROCESSING_TIMEOUT` | `30s` / `20s` | visibilidade / prazo de processamento (precisa ser menor) |
| `SQS_MAX_RECEIVE_COUNT` | `5` | recebimentos até a DLQ (redrive) |
| `SQS_BACKOFF_BASE` / `SQS_BACKOFF_MAX` | `2s` / `5m` | backoff de falhas transitórias |
| `OUTBOX_ENABLED`, `OUTBOX_WORKERS` | `true`, `4` | publicador da outbox |
| `OUTBOX_POLL_INTERVAL`, `OUTBOX_BATCH_SIZE`, `OUTBOX_LEASE` | `500ms`, `50`, `30s` | varredura, lote, lease |
| `OUTBOX_BACKOFF_BASE` / `OUTBOX_BACKOFF_MAX` | `1s` / `5m` | backoff de publicação |
| `PENDING_ENABLED`, `PENDING_POLL_INTERVAL`, `PENDING_BATCH_SIZE` | `true`, `1s`, `50` | worker de referências pendentes |
| `PENDING_BACKOFF_BASE` / `PENDING_BACKOFF_MAX` | `2s` / `1m` | backoff exponencial |
| `PENDING_MAX_ATTEMPTS` / `PENDING_TTL` | `10` / `15m` | expiração da pendência |
| `OIDC_ISSUER_URL` | — (obrigatória) | `iss` esperado (`http://keycloak:8080/realms/wagering`) |
| `OIDC_JWKS_URL` | `<issuer>/protocol/openid-connect/certs` | chaves públicas do IdP |
| `OIDC_AUDIENCE` | `wagering-api` | `aud` exigido |
| `FAULT_INJECTION` | vazio | **somente testes**: pontos de crash (veja §9.5) |

A configuração é validada na inicialização (campos obrigatórios, URLs, prazos coerentes); dependências (PostgreSQL, filas, JWKS do IdP) são verificadas nos hooks `OnStart` do Fx.

## 4. Filas

`sqs-provision` (`cmd/sqs-provision`) é idempotente e cria:

| Fila | Tipo | Configuração |
| --- | --- | --- |
| `wager-transactions.fifo` | entrada | visibility 30s, redrive → `wager-transactions-dlq.fifo` com `maxReceiveCount=5` |
| `wager-transactions-dlq.fifo` | DLQ | recebe redrive e envios explícitos de mensagens inválidas |
| `wallet-events.fifo` | saída (eventos de integração) | redrive → `wallet-events-dlq.fifo` |

Além disso aplica políticas de acesso por credencial (produtores só enviam para a fila de entrada; o serviço consome a entrada, escreve na DLQ e publica eventos; consumidores downstream só leem eventos). Para reexecutar manualmente:

```sh
docker compose run --rm sqs-provision
docker compose exec localstack awslocal sqs list-queues
```

## 5. Migrations

Migrations versionadas em [`migrations/`](migrations) (formato golang-migrate, embutidas no binário).

```sh
# aplicar (o compose já faz isso no serviço `migrate`)
docker compose run --rm migrate                      # = /app/migrate up
# reverter a última / N / todas
docker compose run --rm --entrypoint /app/migrate migrate down
docker compose run --rm --entrypoint /app/migrate migrate down 2
docker compose run --rm --entrypoint /app/migrate migrate down-all
# versão atual / marcar versão limpa após falha
docker compose run --rm --entrypoint /app/migrate migrate version
docker compose run --rm --entrypoint /app/migrate migrate force 2
```

Fora do Docker: `MIGRATIONS_DATABASE_URL=postgres://wagering_owner:wagering_owner_local@localhost:55432/wagering?sslmode=disable go run ./cmd/migrate up`.

| Versão | Conteúdo |
| --- | --- |
| `000001_wallet_schema` | tabelas, constraints, índices únicos parciais e triggers de proteção |
| `000002_app_role_grants` | privilégios mínimos do papel `wallet_app` (sem `UPDATE`/`DELETE` no ledger) |

## 6. Executando a aplicação

- **Compose (recomendado):** `docker compose up --build` sobe as três instâncias.
- **Local, fora do Docker** (com as dependências do compose rodando):

```sh
docker compose up -d postgres keycloak localstack migrate sqs-provision
DATABASE_URL='postgres://wallet_app:wallet_app_local@localhost:55432/wagering?sslmode=disable' \
OIDC_ISSUER_URL=http://keycloak:8080/realms/wagering \
OIDC_JWKS_URL=http://localhost:8180/realms/wagering/protocol/openid-connect/certs \
SQS_ENDPOINT=http://localhost:4566 AWS_ACCESS_KEY_ID=wallet-service AWS_SECRET_ACCESS_KEY=x \
HTTP_ADDR=:8090 go run ./cmd/wallet-service
```

O `SIGTERM`/`SIGINT` dispara o shutdown gracioso (veja ARCHITECTURE.md §10).

## 7. Autenticação e identidades de teste

O realm `wagering` é importado automaticamente. Todos os clientes usam `client_credentials`; o segredo local é `<client-id>-secret`.

| Cliente | Papel | `provider_id` | Uso |
| --- | --- | --- | --- |
| `provider-a` | `wagering-provider` | `provider-a` | provedor A |
| `provider-b` | `wagering-provider` | `provider-b` | provedor B (testes de isolamento) |
| `wallet-service` | `wallet-operator` | — | serviço interno: abrir/ler/reconciliar carteiras |
| `provider-a-shortlived` | `wagering-provider` | `provider-a` | token de 5s (teste de expiração) |
| `no-role-client` | — | — | autenticado sem permissões (403) |
| `other-audience-client` | `wagering-provider` | `provider-a` | token sem `aud=wagering-api` (401) |

O token é emitido com `iss=http://keycloak:8080/realms/wagering` tanto para chamadas de dentro da rede Docker quanto do host (`KC_HOSTNAME` fixo).

```sh
token() { curl -s -d grant_type=client_credentials -d client_id=$1 -d client_secret=$1-secret \
  http://localhost:8180/realms/wagering/protocol/openid-connect/token | sed -E 's/.*"access_token":"([^"]+)".*/\1/'; }
OP=$(token wallet-service); PA=$(token provider-a); PB=$(token provider-b)
```

Console administrativo: http://localhost:8180 (`admin`/`admin`).

## 8. Exemplos de chamadas

```sh
PLAYER=0192f28f-5dc0-7d58-bdb2-814ad6a0f4a1

# Abrir carteira (serviço interno) -> 201
curl -s -XPOST localhost:8081/wallets -H "Authorization: Bearer $OP" -H 'Content-Type: application/json' \
  -d "{\"playerId\":\"$PLAYER\",\"initialBalance\":{\"amount\":\"1000.00\",\"currency\":\"BRL\"}}"
# {"id":"<WALLET>","playerId":"0192f28f-...","balance":{"amount":"1000.00","currency":"BRL"},"version":1}
WALLET=<id retornado>

# Aposta (provedor A) em qualquer instância -> 200
BODY="{\"providerId\":\"provider-a\",\"externalTransactionId\":\"transaction-123\",\"playerId\":\"$PLAYER\",\"walletId\":\"$WALLET\",\"roundId\":\"round-987\",\"gameId\":\"fortune-chimp\",\"kind\":\"BET\",\"money\":{\"amount\":\"25.00\",\"currency\":\"BRL\"}}"
curl -s -XPOST localhost:8082/wagering/transactions -H "Authorization: Bearer $PA" \
  -H 'Idempotency-Key: provider-a:transaction-123' -H 'Content-Type: application/json' -d "$BODY"
# {"transactionId":"...","status":"PROCESSED","balance":{"amount":"975.00","currency":"BRL"},"idempotentReplay":false}

# Reenvio (outra instância) -> mesmo resultado, idempotentReplay=true
curl -s -XPOST localhost:8083/wagering/transactions -H "Authorization: Bearer $PA" \
  -H 'Idempotency-Key: provider-a:transaction-123' -H 'Content-Type: application/json' -d "$BODY"

# Reembolso: acrescente "referenceExternalTransactionId":"transaction-123" e kind REFUND

# Consultas
curl -s localhost:8081/wallets/$WALLET -H "Authorization: Bearer $OP"
curl -s "localhost:8081/wallets/$WALLET/ledger?limit=50" -H "Authorization: Bearer $OP"
curl -s localhost:8081/wagering/transactions/<transactionId> -H "Authorization: Bearer $PA"
curl -s localhost:8081/providers/provider-a/wagering/transactions/transaction-123 -H "Authorization: Bearer $PA"

# Reconciliação -> {"storedBalance":975.00,"calculatedBalance":975.00,"difference":0.00,"consistent":true,"checkedEntries":2}
curl -s -XPOST localhost:8081/wallets/$WALLET/reconciliation -H "Authorization: Bearer $OP"

# Mesma operação via SQS
docker compose exec localstack awslocal sqs send-message \
  --queue-url http://localhost:4566/000000000000/wager-transactions.fifo \
  --message-group-id $WALLET --message-deduplication-id msg-124 \
  --message-body "{\"messageId\":\"msg-124\",\"type\":\"WagerTransactionRequested\",\"occurredAt\":\"2026-09-08T12:00:00.000Z\",\"data\":{\"providerId\":\"provider-a\",\"externalTransactionId\":\"transaction-124\",\"idempotencyKey\":\"provider-a:transaction-124\",\"playerId\":\"$PLAYER\",\"walletId\":\"$WALLET\",\"roundId\":\"round-987\",\"gameId\":\"fortune-chimp\",\"kind\":\"BET\",\"money\":{\"amount\":\"25.00\",\"currency\":\"BRL\"}}}"

# Eventos publicados
docker compose exec localstack awslocal sqs receive-message --max-number-of-messages 10 \
  --queue-url http://localhost:4566/000000000000/wallet-events.fifo
```

### 8.1. Contrato de respostas

| Situação | HTTP | Corpo |
| --- | --- | --- |
| Operação processada (nova ou replay) | `200` | `{"transactionId","status":"PROCESSED","balance","idempotentReplay"}` |
| Aguardando referência (`PENDING_REFERENCE`) | `202` | `{"transactionId","status":"PENDING_REFERENCE","idempotentReplay"}` |
| Rejeição de negócio (terminal, persistida) | `422` | `{"transactionId","status":"REJECTED","failureCode","balance"?,"idempotentReplay"}` |
| Falha permanente registrada (`FAILED`) | `500` | `{"transactionId","status":"FAILED","failureCode"}` |
| Entrada inválida (nada é persistido) | `400` | `{"error":{"code","message","correlationId"}}` — ex.: `INVALID_AMOUNT`, `KIND_NOT_ALLOWED`, `MISSING_IDEMPOTENCY_KEY`, `MALFORMED_REQUEST` |
| Carteira/transação inexistente | `404` | `{"error":{"code":"WALLET_NOT_FOUND" \| "TRANSACTION_NOT_FOUND"}}` |
| Conflito | `409` | `IDEMPOTENCY_KEY_CONFLICT`, `EXTERNAL_TRANSACTION_CONFLICT`, `WALLET_ALREADY_EXISTS` |
| Sem token / token inválido / expirado | `401` | `UNAUTHENTICATED`, `INVALID_TOKEN`, `TOKEN_EXPIRED` |
| Sem permissão / provedor divergente | `403` | `FORBIDDEN`, `PROVIDER_MISMATCH` |
| Indisponibilidade transitória (banco, lock timeout, conflitos esgotados) | `503` + `Retry-After: 1` | `TEMPORARILY_UNAVAILABLE` — repita com a mesma `Idempotency-Key` |

Todas as respostas trazem `X-Correlation-Id` (propagado se enviado pelo cliente).

### 8.2. Códigos de rejeição (`failureCode`)

| Código | Classe | Significado |
| --- | --- | --- |
| `INSUFFICIENT_FUNDS` | resultado definitivo | `BET` maior que o saldo |
| `REVERSAL_INSUFFICIENT_FUNDS` | resultado definitivo | `ROLLBACK` de `WIN`/`REFUND` precisaria deixar o saldo negativo |
| `REFERENCE_ALREADY_REVERSED` | resultado definitivo | a referência já recebeu uma reversão (`REFUND` ou `ROLLBACK`) |
| `REFERENCE_NOT_PROCESSED` | resultado definitivo | a referência terminou `REJECTED`/`FAILED`, ou seguia pendente quando a espera expirou |
| `REFERENCE_NOT_FOUND` | resultado definitivo | a referência não chegou dentro de `PENDING_MAX_ATTEMPTS`/`PENDING_TTL` |
| `WALLET_PLAYER_MISMATCH` | entrada corrigível | carteira não pertence ao `playerId` (saldo não é revelado) |
| `WALLET_CURRENCY_MISMATCH` | entrada corrigível | moeda diferente da carteira |
| `REFERENCE_MISMATCH` | entrada corrigível | referência de outro jogador/carteira/moeda/rodada |
| `REFERENCE_AMOUNT_MISMATCH` | entrada corrigível | valor diferente do referenciado (reversão parcial) |
| `REFERENCE_KIND_INVALID` | entrada corrigível | tipo referenciado inválido (ex.: `REFUND` de `WIN`) |
| `PERMANENT_PROCESSING_ERROR` | `FAILED` | falha permanente não transitória na retomada de uma pendência |

"Entrada corrigível" significa que o provedor deve enviar **uma nova operação** (novo `externalTransactionId`) com os dados corretos: a operação rejeitada é terminal e o replay sempre devolve a rejeição.

## 9. Testes

### 9.1. Unitários (sem dependências externas)

```sh
go test ./...          # domínio, casos de uso (repositório em memória), contrato, config, grafo Fx
go vet ./...
```

### 9.2. Preparar as dependências dos testes de integração

Os testes de integração usam **PostgreSQL, Keycloak e LocalStack reais** do compose. Cada teste cria **seu próprio banco** (`it_<random>`, com migrations aplicadas) e **suas próprias filas FIFO**, e remove tudo ao final; por isso eles não interferem nas instâncias `app-*` nem entre si.

```sh
docker compose up -d postgres keycloak localstack   # o restante do compose é opcional
```

Endpoints usados (sobrescrevíveis): `IT_PG_ADMIN_URL` (default `postgres://wagering_owner:wagering_owner_local@localhost:55432/wagering`), `IT_KEYCLOAK_URL` (`http://localhost:8180`), `IT_SQS_ENDPOINT` (`http://localhost:4566`), `IT_OIDC_ISSUER`.

### 9.3. Integração (build tag `integration`)

```sh
go test -tags integration -count=1 ./test/integration/...
go test -tags integration -count=1 -run TestConcurrency -v ./test/integration/...   # um grupo
```

### 9.4. Com `-race`

```sh
go test -race ./...                                   # unitários
go test -race -tags integration -count=1 ./...        # tudo (requer CGO + compilador C)
```

Sem compilador C no host (ex.: Windows), use o runner containerizado, que executa na rede do compose:

```sh
docker compose --profile test run --rm tests                       # = go test -race -tags integration -count=1 ./...
docker compose --profile test run --rm tests -race -tags integration -run TestConcurrency -v ./test/integration
```

### 9.5. Múltiplas instâncias e simulações de falha

Os testes de múltiplas instâncias compilam `cmd/wallet-service` e iniciam **processos independentes** do sistema operacional (memória e pool próprios) contra o mesmo banco e as mesmas filas. As falhas são injetadas por `FAULT_INJECTION`, que encerra o processo com `exit 137` (equivalente a `SIGKILL`, sem hooks de shutdown) num ponto exato:

| Ponto | Onde o processo morre |
| --- | --- |
| `consumer.after-commit` | após o commit do tratamento da mensagem, antes do `DeleteMessage` |
| `outbox.after-publish` | após publicar no SQS, antes de marcar `published_at` |
| `outbox.after-claim` | após obter o lease dos eventos, antes de publicar |
| `http.after-commit` | após o commit, antes de responder ao cliente |

Indisponibilidade de PostgreSQL/SQS é simulada com um proxy TCP no teste que derruba e restabelece as conexões.

| Requisito (§13 do desafio) | Teste |
| --- | --- |
| 1. mesma aposta 50× em paralelo | `TestConcurrency*/same_bet_50_times…` (1 débito, 49 replays comprovados) |
| 2. duas apostas de 80.00 sobre 100.00 | `TestConcurrency*/two_bets_of_80.00…` (10 rodadas, com reenvio) |
| 3. carteiras distintas em paralelo | `TestConcurrency*/independent_wallets…` (carteira travada não bloqueia as demais) |
| 4. três instâncias independentes | `TestConcurrencyThreeIndependentProcesses` (+ HTTP e SQS concorrentes) |
| 5. consumidor morre após commit e antes da remoção | `TestConsumerCrashAfterCommitBeforeDelete` |
| 6. dois publishers + recuperação de publicação | `TestOutboxCompetingPublishersAndRecovery` |
| 7. reversão antes da referência / expiração | `TestReversalBeforeReference` |
| 8. reinício, `PENDING` confirmado retomado por outra instância | `TestRestartPreservesIdempotencyAndPendingWork` |
| retry, DLQ, mensagens inválidas, hash divergente | `TestDeadLetterQueue` |
| indisponibilidade de PostgreSQL e SQS | `TestDependencyOutages` |
| migrations, constraints, imutabilidade, atomicidade | `TestMigrationsApplyAndRevert`, `TestLedgerIsAppendOnly`, `TestSchemaEnforcesFinancialInvariants`, `TestSingleReversalIsEnforcedByTheDatabase`, `TestFinancialChangesAreAtomic` |
| autenticação real e isolamento entre provedores | `TestAuthentication`, `TestAuthorizationAndProviderIsolation` |
| composição Fx, início/encerramento, liberação de recursos | `TestCompositionIsValid` (unitário), `TestFxLifecycleStartsAndReleasesResources`, `TestGracefulShutdownFinishesInFlightMessage` |
| reconciliação e divergência | `TestReconciliationAndLedgerPagination` |

Todos os cenários terminam conferindo saldo armazenado = créditos − débitos do ledger.

### 9.6. Teste de carga (opcional)

```sh
docker compose up --build -d
go run ./cmd/loadtest -wallets 50 -ops 5000 -concurrency 32 -duplicates 0.1
```

Metodologia: 50 carteiras com 1000.00; 5000 operações (60% `BET` 1.00, 30% `WIN` 1.50, 10% `LOSS`), 10% delas reenvios de operações anteriores; 32 clientes concorrentes distribuídos em round-robin pelas 3 instâncias; ao final espera a outbox esvaziar e reconcilia todas as carteiras.

Resultado de referência (notebook i7-1165G7 4c/8t, 20 GB, Windows 11 + Docker Desktop 28.5 com 8 vCPU/10 GB; tudo na mesma máquina):

```
instances:            3
wallets / ops / conc: 50 / 5000 / 32 (duplicates 10%)
duration:             8.667s
throughput:           576.9 ops/s
latency p50/p95/p99:  45.3ms / 132.3ms / 202.5ms (max 476ms)
http statuses:        200=5000
concurrency conflicts: 0 (retried internally)
outbox lag:           max 3158ms during the run; drained 2.12s after the last request
reconciliation:       50/50 wallets consistent
```

## 10. Observabilidade

- **Logs** JSON (`log/slog`) com `correlationId`, `messageId`, `transactionId`, `walletId`, `providerId`, `instance`; sem tokens, segredos ou payloads financeiros completos.
- **Métricas** Prometheus em `GET /metrics`: `wager_transactions_total{channel,kind,status}`, `wager_idempotent_replays_total`, `wager_idempotency_conflicts_total`, `inbox_duplicate_messages_total`, `retries_total{component}`, `sqs_dlq_messages_total{reason}`, `sqs_messages_total{outcome}`, `concurrency_conflicts_total`, `outbox_lag_seconds`, `outbox_pending_events`, `outbox_publish_delay_seconds`, `outbox_publish_failures_total`, `wager_processing_duration_seconds`, `reconciliation_divergences_total`, `auth_failures_total{reason}`, `http_requests_total`, `http_request_duration_seconds`.
- **Health:** `GET /health/live` (processo) e `GET /health/ready` (PostgreSQL + SQS; `503` durante o shutdown).

## 11. Estrutura

```
cmd/wallet-service   binário do serviço (Fx)            cmd/migrate        migrations up/down
cmd/sqs-provision    filas, DLQs, redrive e políticas   cmd/loadtest       teste de carga
internal/domain      money, wallet, wager, events, derr (sem Fx/HTTP/SQS/SQL)
internal/app         casos de uso e portas (UnitOfWork, repositórios, métricas)
internal/infra       postgres (pgx + SQL explícito), sqsx (AWS SDK v2)
internal/worker      consumidor SQS, publicador da outbox, worker de pendências
internal/httpapi     rotas, autenticação/autorização, contrato de erros
internal/auth        validação de tokens OIDC (JWKS do Keycloak)
internal/bootstrap   módulos Fx e ciclo de vida
migrations/          SQL versionado          deploy/       Keycloak realm, init do PostgreSQL
test/integration     testes de integração (tag `integration`)
```

Qualidade: `gofmt -l .` vazio, `go vet ./...` e `go vet -tags integration ./...` limpos, dependências fixadas em `go.mod`/`go.sum`.
