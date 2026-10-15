# Arquitetura e decisões técnicas

Este documento registra as decisões sobre dinheiro, transações, idempotência, locks, referências pendentes, reversões, inbox/outbox, autenticação, autorização, uso do Fx e shutdown, além das limitações e interpretações adotadas. Instruções de execução estão no [README](README.md).

## 1. Visão geral

```
                 HTTP (OAuth2 Bearer)                     SQS FIFO wager-transactions.fifo
                        │                                              │
        ┌───────────────▼──────────────┐              ┌────────────────▼───────────────┐
        │ httpapi (auth, contrato)     │              │ worker.Consumer (inbox, DLQ)   │
        └───────────────┬──────────────┘              └────────────────┬───────────────┘
                        └──────────────┐            ┌──────────────────┘
                                ┌──────▼────────────▼──────┐
                                │ app.WageringService      │  mesmo caso de uso e mesmas
                                │ app.WalletService        │  garantias para HTTP e SQS
                                └──────┬───────────────────┘
                                       │ UnitOfWork (1 transação SQL)
           ┌───────────────────────────▼────────────────────────────────┐
           │ PostgreSQL: wallets · wager_transactions · ledger ·         │
           │             inbox_messages · outbox_events                  │
           └───────────────┬───────────────────────────────▲────────────┘
                           │ claim/lease                   │ retoma pendências
                 ┌─────────▼─────────┐            ┌────────┴──────────┐
                 │ worker.Outbox     │            │ worker.Pending    │
                 └─────────┬─────────┘            └───────────────────┘
                           ▼
                 SQS FIFO wallet-events.fifo
```

Camadas:

| Pacote | Responsabilidade | Dependências |
| --- | --- | --- |
| `internal/domain/*` | `Money`, `Wallet`, `LedgerEntry`, `WagerTransaction`, eventos, erros classificáveis | só stdlib + `google/uuid` (sem Fx, HTTP, SQS ou SQL) |
| `internal/app` | casos de uso e **portas** (`UnitOfWork`, repositórios, `Metrics`, `Clock`, `IDGenerator`) | domínio |
| `internal/infra/postgres`, `internal/infra/sqsx` | adaptadores (pgx com SQL explícito; AWS SDK v2) | app, domínio |
| `internal/httpapi`, `internal/worker` | entradas (HTTP, consumidor) e workers | app |
| `internal/bootstrap` | composição Fx e ciclo de vida | tudo |

Todo I/O recebe `context.Context` e respeita cancelamento/prazo (`statement_timeout`, `lock_timeout`, prazos por mensagem e por publicação).

## 2. Dinheiro (`Money`)

- **Representação:** `int64` em unidades mínimas (centavos) + moeda ISO 4217. Escala fixa de **2 casas**. Faixa: `±92.233.720.368.547.758,07`. `math.MinInt64` é excluído para que a negação seja sempre representável.
- **Moedas:** códigos ISO 4217 cujo expoente é 2 (BRL, USD, EUR, …). Moedas com outro expoente (JPY, KWD) são rejeitadas em vez de reescaladas. O valor zero de `Currency`/`Money` é inválido e qualquer operação sobre ele retorna `UNINITIALIZED_*`.
- **Parsing:** gramática `-?(0|[1-9][0-9]*)\.[0-9]{2}`, com acumulação dígito a dígito e checagem de overflow. São rejeitados: vazio, `NaN`, `Infinity`, notação científica, `+`, escala diferente de 2 (`25`, `25.0`, `25.001`), zeros à esquerda, espaços, separadores de milhar e `-0.00`. **Nada é arredondado.** Entradas externas usam `ParseNonNegative` (negativos → `NEGATIVE_AMOUNT`). No JSON, `amount` precisa ser *string*: números JSON são recusados para nunca passarem por `float64`.
- **Normalização antes do hash:** como só a forma canônica é aceita, a normalização do valor é a identidade. Moeda deve vir em maiúsculas (sem normalização). UUIDs são aceitos em qualquer caixa e normalizados para minúsculas.
- **Aritmética:** `Add`, `Sub`, `Negate`, `Cmp` exigem moedas iguais (`CURRENCY_MISMATCH`) e detectam overflow (`AMOUNT_OVERFLOW`). Valores negativos existem em cálculos internos (ex.: `difference` da reconciliação), nunca no saldo.
- **Persistência:** `BIGINT` (centavos) + `char(3)`. Somatórios da reconciliação usam `SUM` (acumulador `NUMERIC` no PostgreSQL) e voltam a `int64`. Não há `float` em nenhum ponto (parsing, cálculo, serialização, persistência).

## 3. Persistência e delimitação das transações

- **Biblioteca:** `pgx/v5` com SQL explícito (sem ORM). Migrations com `golang-migrate` (arquivos versionados, embutidos no binário).
- **Unidade de trabalho:** `app.UnitOfWork.Within(ctx, fn)` abre **uma** transação `READ COMMITTED` e entrega a `fn` um `app.Tx` cujos repositórios (`Wallets`, `Transactions`, `Ledger`, `Outbox`, `Inbox`) compartilham a mesma `pgx.Tx`. Commit só se `fn` retornar `nil`; qualquer erro faz rollback. É assim que estado da operação, saldo, ledger, inbox e outbox são confirmados atomicamente.
- **Reconciliação** usa `UnitOfWork.Snapshot` (`REPEATABLE READ READ ONLY`): saldo e soma do ledger vêm da mesma visão.
- **Classificação de erros de infraestrutura:** `postgres.Classify` converte erros do driver em `app.ErrTransient` (conexão, timeout, `lock_timeout`, `57xxx`, `08xxx`, `53300`) ou `app.ErrConcurrencyConflict` (`40001`, `40P01`, versão alterada). Violações de constraint e erros de domínio passam intactos.
- **Papel de menor privilégio:** o serviço conecta como `wallet_app`, sem `UPDATE`/`DELETE` no ledger e sem `DELETE` em carteiras/transações. As migrations rodam com o dono do schema.

### 3.1. Invariantes impostas pelo banco

| Invariante | Mecanismo |
| --- | --- |
| Saldo ≥ 0 | `CHECK (balance_minor >= 0)` |
| Uma carteira por `(playerId, currency)` | `UNIQUE (player_id, currency)` |
| Ledger append-only | triggers `BEFORE UPDATE/DELETE/TRUNCATE` (bloqueiam inclusive o dono) + ausência de privilégio para `wallet_app` |
| `balanceAfter = balanceBefore ± amount` | `CHECK` por direção; `amount > 0`; saldos ≥ 0 |
| Um lançamento por `(walletId, transactionId)` | `UNIQUE` |
| Sem *lost update* | `UNIQUE (wallet_id, wallet_version)` no ledger: dois escritores nunca aplicam a mesma "versão + 1" |
| Toda mudança de saldo tem lançamento | *constraint trigger* `DEFERRABLE INITIALLY DEFERRED` em `wallets`: no commit, exige o lançamento da versão atual com `balance_after` igual ao saldo; versão só muda junto com o saldo e sempre +1 |
| Idempotência | `UNIQUE (provider_id, idempotency_key)` e `UNIQUE (provider_id, external_transaction_id)` |
| Um `OPENING` por carteira | índice único parcial `(wallet_id) WHERE kind='OPENING'` |
| Uma reversão bem-sucedida por referência | índice único parcial `(reference_transaction_id) WHERE status='PROCESSED' AND kind IN ('REFUND','ROLLBACK')` |
| Forma interna × externa | `CHECK` (`OPENING` ⇔ `INTERNAL` sem metadados externos; externas exigem provedor, ids, chave, hash, rodada, jogo) |
| Política de valores | `LOSS` = 0, demais > 0; referência obrigatória para `REFUND`/`ROLLBACK`, proibida para `BET`/`LOSS`/`OPENING` |
| Estados terminais imutáveis | trigger em `wager_transactions`: sem updates após `PROCESSED/REJECTED/FAILED`; campos de negócio imutáveis; sem `DELETE` |
| Outbox imutável | trigger: payload/tipo/ids imutáveis; evento publicado não "despublica" |

## 4. Concorrência

**Estratégia: lock pessimista por carteira + atualização condicionada por versão + constraints.**

1. `SELECT … FROM wallets WHERE id = $1 FOR UPDATE` serializa somente as escritas da **mesma carteira**; carteiras diferentes avançam em paralelo (não há lock global, nem advisory lock, nem mutex em memória).
2. O agregado `Wallet` calcula o novo saldo; o repositório grava com `UPDATE … WHERE id = $1 AND version = $persistida`. Zero linhas afetadas ⇒ `ErrConcurrencyConflict` ⇒ rollback e nova tentativa (até `CONFLICT_RETRIES`, com jitter). Na prática isso nunca dispara com o lock, mas protege contra qualquer caminho que não o adquira.
3. O banco garante o resto (§3.1), independentemente dos locks e da deduplicação do SQS FIFO.

Por que pessimista: a contenção é por carteira e as transações são curtas; o lock evita retrabalho e torna a disputa determinística (no teste 80/80 a segunda aposta vê o saldo de 20.00 e é rejeitada com `INSUFFICIENT_FUNDS`, sem retries).

**Ordem de locks (livre de deadlock):** caminho síncrono: carteira → inserção da transação. Worker de pendências: carteira (`FOR UPDATE SKIP LOCKED`) → linha da transação. A "despertada" de reversões aguardando uma referência (`WakeWaiting`) ocorre com a carteira já travada. Deadlocks/serialização residuais são reclassificados e repetidos.

**Esperas:** `lock_timeout=5s` e `statement_timeout=10s`; estourou ⇒ `503 TEMPORARILY_UNAVAILABLE` (HTTP) ou retry com backoff (SQS).

As garantias foram verificadas com **três processos independentes** (memória e pools próprios) e com HTTP e SQS concorrendo pela mesma operação.

## 5. Idempotência

- **Chave:** HTTP usa o header `Idempotency-Key` (obrigatório, nunca substituído por um valor calculado); SQS usa `data.idempotencyKey`. A unicidade é **por provedor**: `(providerId, idempotencyKey)`; além disso `(providerId, externalTransactionId)` não pode ser reaplicado com outra chave.
- **Persistência:** a própria linha de `wager_transactions` é o registro de idempotência (sobrevive a reinícios; nada em memória).
- **Hash do payload:** SHA-256 (hex) do **JSON canônico** — objeto com chaves ordenadas lexicograficamente, sem espaços, sem *HTML escaping*, todos os valores como string:

  `gameId, kind, money.amount, money.currency, playerId, providerId, externalTransactionId, referenceExternalTransactionId (só se presente), roundId, walletId`

  Excluídos: a chave de idempotência e todo metadado de transporte (headers, `messageId`, `occurredAt`, `type`, correlation ids). Normalizações: UUIDs em minúsculas; dinheiro na forma canônica (única aceita). HTTP e SQS produzem o mesmo hash para a mesma operação (há teste com UUID em maiúsculas no SQS).
- **Regras:**
  - mesma chave + mesmo hash ⇒ devolve o resultado persistido com `idempotentReplay: true` (inclusive o **saldo observado no processamento original**, guardado em `result_balance_minor`);
  - mesma chave + hash diferente ⇒ `409 IDEMPOTENCY_KEY_CONFLICT`;
  - mesmo `externalTransactionId` com outra chave ⇒ `409 EXTERNAL_TRANSACTION_CONFLICT`.
- **Corrida entre duplicatas:** (1) leitura rápida sem lock para replays; (2) lock da carteira; (3) `INSERT … ON CONFLICT DO NOTHING` reivindica a chave — se outra transação segura a mesma chave, o PostgreSQL espera seu desfecho; (4) em conflito, uma **única consulta** (`idempotency_key = $k OR external_transaction_id = $e`) decide entre replay e conflito. Uma versão anterior fazia duas consultas separadas e, sob `READ COMMITTED`, um commit concorrente entre elas fazia um replay parecer conflito de `externalTransactionId`; o teste de 50 duplicatas sob `-race` expôs o problema.
- **Entradas inválidas** (400/404) não persistem nada: podem ser corrigidas e reenviadas com a mesma chave.

## 6. Máquina de estados da `WagerTransaction`

```
               ┌──────────────────────────────────────────┐
               │                                          ▼
  (novo) ─► PENDING ──► PENDING_REFERENCE ──(retry)──► PENDING_REFERENCE
               │              │
               │              ├──► PROCESSED   (terminal)
               ├──────────────┼──► REJECTED    (terminal, failureCode)
               └──────────────┴──► FAILED      (terminal, failureCode)
```

- Transições só pelos métodos do agregado (`Execute`, `MarkRejected`, `MarkPendingReference`, `MarkFailed`, `CompleteOpening`), que validam a origem e registram os eventos. Terminal ⇒ `INVALID_TRANSITION` (domínio) e trigger no banco.
- **Criação × reidratação:** `NewExternal`/`NewOpening` validam e criam em `PENDING`; `Rehydrate` só valida consistência — não reaplica movimentos, transições nem eventos.
- **Processamento síncrono:** operações sem dependência nascem `PENDING` e terminam `PROCESSED`/`REJECTED` **na mesma transação SQL** (sem commit intermediário de aceite). Assim, um `PENDING` só é confirmado se algum caminho o fizer explicitamente; ainda assim o worker de pendências retoma **qualquer** `PENDING` confirmado (há teste que insere um `PENDING` "órfão" e verifica a retomada por outra instância).
- **Falhas transitórias × permanentes:**
  - transitórias (`app.ErrTransient`, `ErrConcurrencyConflict`, prazos): nada é registrado como falha; a transação SQL é desfeita e a operação é repetida (HTTP `503` para o cliente repetir; SQS volta à fila com backoff; worker tenta no próximo ciclo);
  - rejeições de negócio: `REJECTED` com `failureCode` estável, terminal;
  - permanentes de infraestrutura (erro inesperado, não transitório, ao retomar uma pendência): `FAILED` com `PERMANENT_PROCESSING_ERROR`, registrado em transação separada para auditoria. No caminho síncrono um erro inesperado faz rollback (`500` / retry do SQS até a DLQ) sem persistir nada.

## 7. Operações, referências e reversões

| Tipo | Efeito | Regras |
| --- | --- | --- |
| `BET` | débito | valor > 0; saldo insuficiente ⇒ `INSUFFICIENT_FUNDS` |
| `WIN` | crédito | valor > 0; referência opcional, que deve ser `BET` da mesma rodada |
| `LOSS` | nenhum | valor exatamente `0.00`; sem ledger, sem mudança de versão; emite só `WagerTransactionProcessed` |
| `REFUND` | crédito | referencia `BET` processada; mesmo valor |
| `ROLLBACK` | contrário ao original | `BET` ⇒ crédito; `WIN`/`REFUND` ⇒ débito; mesmo valor |
| `OPENING` | crédito inicial | interno; rejeitado em HTTP/SQS (`KIND_NOT_ALLOWED`) |

Validações da referência (resolvida por `(providerId, referenceExternalTransactionId)`): mesmo provedor, jogador, carteira, moeda e rodada (`REFERENCE_MISMATCH`), tipo permitido (`REFERENCE_KIND_INVALID`), valor igual para reversões (`REFERENCE_AMOUNT_MISMATCH`; reversões parciais não são suportadas).

**Política REFUND × ROLLBACK (uma reversão por débito):** cada transação recebe **no máximo uma reversão bem-sucedida, seja `REFUND` ou `ROLLBACK`**. Assim, uma `BET` reembolsada não pode sofrer `ROLLBACK` (e vice-versa): `REFERENCE_ALREADY_REVERSED`. Um `REFUND` pode, ele próprio, sofrer `ROLLBACK` (debita a devolução de volta); isso **não** libera a `BET` para uma nova reversão, pois o `REFUND` original continua `PROCESSED` e ocupa o "slot" de reversão da aposta. `ROLLBACK` de `ROLLBACK` não é permitido. Resultado: um mesmo débito nunca é devolvido duas vezes. A regra é verificada no domínio (com a carteira travada) e garantida pelo índice único parcial.

**Reversão sem saldo:** `ROLLBACK` de `WIN`/`REFUND` que deixaria saldo negativo ⇒ `REJECTED` com `REVERSAL_INSUFFICIENT_FUNDS` (código distinto de `INSUFFICIENT_FUNDS`), evento de rejeição e auditoria na própria transação.

### 7.1. Referências ainda indisponíveis

- Referência ausente ⇒ `PENDING_REFERENCE` com `next_attempt_at`, evento `WagerTransactionPendingReference` (só na primeira vez), HTTP `202`. A mensagem SQS de entrada é concluída (inbox + delete) depois que a pendência está persistida; o worker assume a continuidade.
- **Worker** (`worker.PendingWorker`, em todas as instâncias): busca pendências vencidas, trava a carteira com `SKIP LOCKED`, relê a transação e reexecuta as regras. Backoff exponencial `PENDING_BACKOFF_BASE·2^n` limitado a `PENDING_BACKOFF_MAX`. Todo o estado está no banco, então sobrevive a reinícios e é compartilhado entre instâncias.
- **Chegada da referência:** quando uma operação é processada, as pendências que a aguardam têm `next_attempt_at` antecipado para "agora" na mesma transação (resolução rápida, sem esperar o backoff).
- **Expiração:** após `PENDING_MAX_ATTEMPTS` tentativas ou `PENDING_TTL` desde a criação ⇒ `REJECTED` com `REFERENCE_NOT_FOUND` + `WagerTransactionRejected`.
- **Referência existe mas está pendente:** continua aguardando; se expirar nessa situação ⇒ `REFERENCE_NOT_PROCESSED`.
- **Referência terminou sem sucesso** (`REJECTED`/`FAILED`) ⇒ rejeição imediata `REFERENCE_NOT_PROCESSED`.

## 8. Abertura de carteira

`POST /wallets` (somente o serviço interno) cria, **num único commit**: a carteira (versão 1), e — se o saldo inicial for positivo — a transação `OPENING` interna já `PROCESSED`, o lançamento de crédito `0 → saldo` (versão 1) e os registros de outbox `WagerTransactionProcessed` e `WalletBalanceChanged` (sem metadados externos). Saldo inicial zero não cria `OPENING`, ledger nem eventos. Segundo pedido para o mesmo jogador e moeda ⇒ `409 WALLET_ALREADY_EXISTS`. O crédito inicial duplicado é impossível (índice único parcial de `OPENING` por carteira).

## 9. Mensageria

### 9.1. Consumidor (`wager-transactions.fifo`)

Para cada mensagem, **na mesma transação SQL** das mudanças de domínio, ledger e outbox:

1. `INSERT` em `inbox_messages (consumer_name, message_id)` — a identidade durável é o `messageId` do envelope; se já existe, compara o hash (divergente ⇒ `MESSAGE_HASH_MISMATCH` ⇒ DLQ; igual ⇒ duplicata, responde com o resultado persistido);
2. caso de uso compartilhado com o HTTP (idempotência por `data.idempotencyKey`);
3. `completed_at`/`outcome` na inbox; commit;
4. **só então** `DeleteMessage`.

Hash da inbox: SHA-256 de `type`, `idempotencyKey` e do hash de negócio (§5); `occurredAt` não participa.

| Situação | Ação |
| --- | --- |
| Processada, rejeição de negócio confirmada, pendência registrada ou duplicata | delete |
| Mensagem inválida (JSON, tipo, campos, valores, `OPENING`), conflito de idempotência/hash, carteira inexistente | **enviada explicitamente à DLQ** (atributos `failureReason`, `failureCode`, `originalMessageId`) e removida — reprocessar não corrige |
| Falha transitória ou erro inesperado | `ChangeMessageVisibility` = `SQS_BACKOFF_BASE·2^(receiveCount-1)` (máx. `SQS_BACKOFF_MAX`); após `maxReceiveCount` (5) recebimentos o **redrive** move para a DLQ |

Limites: visibility timeout 30s; prazo de processamento por mensagem 20s (menor que a visibilidade, validado na configuração); long polling 10s; até 10 mensagens por receive, processadas em ordem. Se uma mensagem de um grupo falha, as seguintes do mesmo grupo no lote são liberadas (visibilidade 0) para preservar a ordem FIFO.

**`MessageGroupId` = `walletId`**: ordem por carteira e paralelismo entre carteiras. **`MessageDeduplicationId`**: definido pelo produtor (recomendado: o `messageId` do envelope ou a chave de idempotência). A deduplicação do SQS (janela de 5 min) é só uma otimização — a correção vem da inbox e da idempotência no banco.

**Shutdown:** o `Stop` cancela o long polling (não busca mais trabalho); a mensagem em processamento continua com seu próprio prazo e é concluída; mensagens recebidas e ainda não iniciadas têm a visibilidade zerada para reentrega imediata a outra instância; se o prazo estourar durante o desligamento a visibilidade também é zerada.

### 9.2. Outbox

- Os eventos são gravados em `outbox_events` (payload `json` — snapshot imutável, exatamente o que é publicado) na mesma transação da mudança de estado. **Nada é publicado antes do commit.**
- `worker.OutboxPublisher` (4 goroutines por instância, todas as instâncias competem): `UPDATE … SET locked_by, locked_until = now()+lease, attempts+1 … WHERE event_id IN (SELECT … FOR UPDATE SKIP LOCKED)` — leases usam o relógio do banco. Publica com `SendMessageBatch` (até 10), marca `published_at` em lote. Falha ⇒ `next_attempt_at = now() + backoff(attempts)` e `last_error`. Sem limite de tentativas: um evento confirmado nunca é descartado (o atraso aparece em `outbox_lag_seconds`).
- **Trabalho abandonado:** publisher morto ⇒ o lease expira (30s) e outra instância reivindica. No shutdown os leases são liberados explicitamente.
- **Crash entre commit e publicação:** o evento continua pendente e é publicado por outra instância. **Crash entre publicação e confirmação:** o evento é republicado com o **mesmo `eventId`** (corpo idêntico, `MessageDeduplicationId = eventId`, atributo `eventId`): o FIFO descarta a duplicata dentro da janela de 5 min e consumidores devem deduplicar por `eventId` fora dela. Ambos os cenários têm testes com crash real do processo.

### 9.3. Contrato dos eventos (`wallet-events.fifo`)

Roteamento: todos os eventos vão para `wallet-events.fifo`, `MessageGroupId = walletId`, `MessageDeduplicationId = eventId`, atributos `eventId`, `eventType`, `aggregateId`. Consumidores: filtrar por `eventType`, deduplicar por `eventId`, ordenar mudanças de saldo por `data.walletVersion` (com vários publishers a ordem entre lotes de uma mesma carteira não é estrita).

Envelope (`version` e `eventType` definidos pelo construtor de cada tipo; timestamps UTC RFC 3339; dinheiro como string decimal):

```json
{"eventId":"uuid","eventType":"WalletBalanceChanged","aggregateId":"…","correlationId":"…","causationId":"…",
 "occurredAt":"2026-09-08T12:00:00.123456Z","version":1,"data":{…}}
```

| Evento | `aggregateId` | `data` |
| --- | --- | --- |
| `WagerTransactionProcessed` | transactionId | `transactionId, origin, kind, walletId, playerId, money, balanceAfter, processedAt` + (externas) `providerId, externalTransactionId, roundId, gameId, referenceExternalTransactionId?, referenceTransactionId?` |
| `WagerTransactionRejected` | transactionId | `transactionId, kind, walletId, playerId, money, providerId, externalTransactionId, roundId, gameId, referenceExternalTransactionId?, failureCode, rejectedAt` |
| `WalletBalanceChanged` | walletId | `walletId, transactionId, direction, money, balanceBefore, balanceAfter, walletVersion` |
| `WagerTransactionPendingReference` | transactionId | `transactionId, kind, walletId, playerId, money, providerId, externalTransactionId, referenceExternalTransactionId, nextAttemptAt` |

`correlationId`: `X-Correlation-Id` do HTTP (ou gerado) / `messageId` (ou atributo `correlationId`) do SQS; `causationId`: `messageId` da mensagem que causou a operação (SQS).

## 10. Composição com Fx, ciclo de vida e shutdown

- Módulos (`fx.Module`): `observability`, `postgres`, `sqs`, `auth`, `app`, `workers`, `http`. Injeção por construtores (`fx.Provide`), registro de servidores/workers por `fx.Invoke`. O domínio e os casos de uso não conhecem o Fx.
- **Início:** `config.Load` valida a configuração antes de construir o app; hooks `OnStart` verificam dependências com retry até o `START_TIMEOUT`: `Ping` no PostgreSQL, resolução das três filas, disponibilidade do JWKS do IdP. Só então os workers e, por último, o listener HTTP sobem.
- **Ordem de parada:** o Fx executa `OnStop` em ordem inversa ao registro. As dependências registram seus hooks ao serem construídas (antes de quem as usa), então o desligamento é:
  1. HTTP: readiness passa a `503` (`draining`), `Server.Shutdown` recusa novas conexões e espera as requisições em andamento;
  2. consumidor SQS: para de buscar; conclui a mensagem em andamento dentro do prazo ou libera a visibilidade;
  3. worker de pendências: termina o item atual (o contexto de cada item é cancelável; o que não terminar é desfeito por rollback e retomado depois);
  4. outbox: termina o lote em envio e libera os leases restantes;
  5. pool do PostgreSQL é fechado por último.
- Cada worker tem término **observável**: `Stop(ctx)` só retorna quando todas as goroutines encerraram (ou devolve erro se o prazo `SHUTDOWN_TIMEOUT` expirar), com log `worker stopped` e duração.
- Testes: `fx.ValidateApp` valida o grafo; um teste de integração sobe o app completo, opera, chama `Stop` e verifica que o listener fechou, que o pool não aceita mais conexões e que as goroutines dos workers terminaram; outro para o app com uma mensagem bloqueada num lock e verifica que o `Stop` espera, o resultado é confirmado e a mensagem é removida.

## 11. Autenticação e autorização

### 11.1. IdP e validação

- **IdP:** Keycloak 26 (OIDC), provisionado automaticamente por import do realm `wagering`. Escolhido por ser o recomendado, open source, rodar localmente e suportar `client_credentials`, mappers de claims e papéis — o necessário para comunicação serviço-a-serviço. Cadastro de senhas e emissão própria de tokens ficam fora do serviço.
- **Fluxo:** `client_credentials`; cada provedor e o serviço interno são clientes confidenciais com *service account*.
- **Validação local (sem introspecção por requisição):** JWT RS256 verificado contra o JWKS do Keycloak (`go-oidc`, cache com refresh ao surgir um `kid` desconhecido), `iss` exato, `exp`, `aud` contendo `wagering-api` (mapper de audiência) e `typ = Bearer` (ID tokens e refresh tokens são recusados). `alg: none` e tokens adulterados são rejeitados. Trade-off: revogação só tem efeito na expiração do token (lifetime de 5 min).
- Emissor fixo (`KC_HOSTNAME=http://keycloak:8080`) e `OIDC_JWKS_URL` separado permitem que o mesmo token valha dentro e fora da rede Docker.

### 11.2. Modelo de permissões

| Papel (realm role) | Pode |
| --- | --- |
| `wagering-provider` + claim `provider_id` | `POST /wagering/transactions` somente com `providerId` = `provider_id` do token; ler **apenas** suas transações (`GET /wagering/transactions/{id}` devolve 404 para transações de outros provedores; `GET /providers/{p}/…` exige `p` = `provider_id`) |
| `wallet-operator` (serviço interno) | abrir, ler, listar ledger e reconciliar carteiras; ler qualquer transação; **não** envia operações de provedor |

- A identidade do provedor vem **exclusivamente** do claim `provider_id`, injetado pelo Keycloak por cliente (mapper *hardcoded claim*); o `providerId` do corpo só é aceito se coincidir. Como a idempotência é indexada por provedor, um provedor nunca obtém o replay/resultado de outro.
- Autorização é verificada antes de qualquer lógica: chamadas negadas não têm efeito financeiro nem expõem dados (testado comparando ledger/saldo antes e depois).
- Health checks e `/metrics` são públicos.

### 11.3. Acesso à mensageria

Cada componente usa credenciais próprias (`wallet-service`, `provisioner`, produtores e consumidores downstream) e o `sqs-provision` aplica **políticas de fila**: produtores só `SendMessage` na fila de entrada; o serviço consome a entrada, escreve na DLQ e publica em `wallet-events.fifo`; consumidores downstream só leem eventos. O consumidor **não confia** no broker: revalida todo o conteúdo como no HTTP (tipos, valores, `OPENING` proibido, carteira/jogador, idempotência).

## 12. Observabilidade

Logs JSON com identificadores propagados por contexto (`correlationId`, `messageId`, `sqsMessageId`, `transactionId`, `walletId`, `providerId`, `clientId`, `instance`); não são registrados tokens, segredos ou payloads completos. Métricas Prometheus (lista no README) cobrem resultados por status, duplicatas, replays, conflitos, retries, DLQ, latência, atraso da outbox e divergências de reconciliação. A reconciliação reporta divergências na resposta, em log `ERROR` e em `reconciliation_divergences_total`, sem alterar o saldo.

## 13. Interpretações adotadas

- **Formato de valores:** só a forma canônica com exatamente duas casas é aceita (`"25"` e `"25.0"` são rejeitados) — elimina ambiguidades de normalização no hash.
- **Códigos HTTP:** `200` processado (inclusive replays), `202` pendente, `422` rejeição de negócio persistida, `400` entrada inválida, `404` carteira inexistente (nada persistido, corrigível), `409` conflitos, `503` transitório.
- **Carteira inexistente** numa operação é tratada como entrada inválida (não há carteira para registrar a transação): não é persistida; no SQS vai para a DLQ.
- **Rejeições por divergência carteira/jogador** não devolvem saldo (evita expor o saldo de outra carteira); por moeda divergente também não (seria em outra moeda).
- **`WIN` com referência** exige `BET` da mesma rodada; `WIN` sem referência é aceito.
- **Replay de `PENDING_REFERENCE`** devolve `202` com o estado atual; quando a pendência termina, o replay devolve o resultado final.
- **`FAILED`** só é produzido na retomada assíncrona; no caminho síncrono uma falha inesperada não persiste estado.
- **Eventos de outbox** não têm limite de tentativas (não se perde evento confirmado); `last_error` e métricas expõem problemas persistentes.

## 14. Limitações e trabalho não concluído

- **IAM no LocalStack Community:** as políticas de fila e as credenciais separadas são aplicadas, mas o LocalStack Community **não as impõe** (`ENFORCE_IAM` é recurso pago). Em AWS real as mesmas políticas passam a valer; a revalidação de domínio no consumidor independe disso.
- **Ordem entre eventos de uma carteira** com vários publishers não é estritamente sequencial; `walletVersion` permite ordenar `WalletBalanceChanged`.
- **Deduplicação de eventos** fora da janela de 5 min do SQS FIFO depende do consumidor (por `eventId`).
- **Reversões parciais**, múltiplas moedas por carteira e moedas com expoente ≠ 2 não são suportadas.
- **`/metrics` é público** na mesma porta; em produção ficaria numa porta interna.
- **Tracing (OpenTelemetry) e dashboards** não foram implementados (opcionais).
- **Ledger de partidas dobradas** não foi implementado (opcional); o ledger é de entrada simples por carteira.
- **Testes com `-race` no Windows** exigem compilador C; por isso há o runner containerizado (`docker compose --profile test run --rm tests`).
