import { useCallback, useEffect, useMemo, useState } from 'react'

import { request } from '@/api/client'

/** 调度页与重跑页共用的依赖沙盘面板：同一份路径票、边序与阻塞水位。 */

export type SandboxTicket = {
  ticket_id: string
  root_task: string
  date_window: { start: string; end: string; days: number }
  dag_version: string
  revision?: number
  status: string
  generated_at: string
  generated_by: string
  blocking_points: {
    blocking_id: string
    type: string
    title: string
    description: string
    edge_id: string | null
    task_id: string | null
    cycle_path: string[] | null
    resolved: boolean
    resolution: { action: string; target: string; remark: string; operator: string; resolved_at: string } | null
    options: { action: string; target: string; label: string; recommended?: boolean }[]
  }[]
  segments: {
    seq: number
    task_id: string
    task_name: string
    layer: string
    target_table: string
    role: string
    in_backfill_scope: boolean
    completed_watermark: string | null
    resume_from: string | null
    blocking_watermark: string | null
    pending_partitions: string[]
    pending_partition_count: number
    simulated_failure_partitions: string[]
    simulated_failure_reason: string
    incoming_edges: {
      edge_id: string
      upstream_task: string
      source: string
      deferred: boolean
      cycle_risk: boolean
      missing_dependency: boolean
    }[]
  }[]
  edge_order: {
    order: number
    edge_id: string
    upstream_task: string
    downstream_task: string
    source: string
    execution_included: boolean
    cycle_risk: boolean
    status?: string
  }[]
  rejected_edges: { edge_id: string; upstream_task: string; downstream_task: string; reject_reason: string }[]
  blocking_watermark: string | null
  blocked_task: string | null
  scope_empty?: boolean
  execution: {
    batch_no: string
    batch_id: string | null
    batch_status: string
    started_at: string | null
    finished_at: string | null
  }
  batch_summary?: {
    batch_no: string
    status: string
    blocking_watermark: string | null
    blocked_task: string | null
    segments: { seq: number; task_id: string; status: string; blocking_watermark: string | null }[]
  }
  formal_dag_untouched: boolean
}

type SandboxContext = {
  dag_version: string
  scenario: { root_task: string; start_date: string; end_date: string; batch_no: string }
  tasks: { task_id: string; task_name: string; layer: string; target_table: string; completed_watermark: string }[]
  candidate_edges: { edge_id: string; upstream_task: string; downstream_task: string; status: string; remark?: string }[]
  latest_ticket_id: string | null
}

type TicketArchive = {
  dag_version: string
  tickets: {
    ticket_id: string
    root_task: string
    date_window: { start: string; end: string }
    status: string
  }[]
}

type BatchDetail = {
  batch_no: string
  status: string
  blocked_task: string | null
  blocking_watermark: string | null
  segments: {
    seq: number
    task_id: string
    task_name: string
    status: string
    blocking_watermark: string | null
    partitions: { partition_date: string; status: string; message: string }[]
  }[]
  edge_order: { order: number; edge_id: string; upstream_task: string; downstream_task: string; status: string }[]
}

const ROLE_LABEL: Record<string, string> = {
  root: '根任务',
  upstream: '上游补跑',
  read_only_upstream: '只读上游',
}

const ACTION_LABEL: Record<string, string> = {
  defer_edge: '暂缓候选边',
  exclude_task: '设为只读上游',
}

async function readJson<T>(path: string, init?: RequestInit): Promise<T> {
  const response = await request(path, init)
  const payload = await response.json().catch(() => ({}))
  if (!response.ok) {
    throw new Error(payload?.detail || '沙盘接口返回异常，数据未更新')
  }
  return payload as T
}

export function SandboxTicketPanel({
  ticket,
  batch,
  compact = false,
  onChanged,
}: {
  ticket: SandboxTicket | null
  batch?: BatchDetail | null
  compact?: boolean
  onChanged?: () => void
}) {
  if (!ticket) {
    return (
      <section className="sandbox-panel" data-sandbox="empty">
        <header className="sandbox-head">
          <h3>依赖沙盘</h3>
          <span className="sandbox-badge">只读推演</span>
        </header>
        <p className="sandbox-hint">尚未生成路径票。请用下方的根任务和分区日期窗生成，同一组合会沿用已有票据。</p>
      </section>
    )
  }
  const batchSegments = batch?.segments ?? []
  const batchEdges = batch?.edge_order ?? []
  const executed = Boolean(ticket.execution.batch_id)

  return (
    <section className="sandbox-panel" data-sandbox="ticket" data-ticket-id={ticket.ticket_id}>
      <header className="sandbox-head">
        <h3>依赖沙盘 · 路径票 {ticket.ticket_id}</h3>
        <span className={`sandbox-badge status-${ticketStatusKey(ticket.status)}`}>{ticket.status}</span>
        <span className="sandbox-meta">DAG 版本 {ticket.dag_version}{ticket.revision ? ` · 修订 ${ticket.revision}` : ''}</span>
        <span className="sandbox-meta">根任务 {ticket.root_task}</span>
        <span className="sandbox-meta">日期窗 {ticket.date_window.start} ~ {ticket.date_window.end}（{ticket.date_window.days} 天）</span>
        <span className="sandbox-meta">生成 {ticket.generated_by} · {ticket.generated_at}</span>
        {ticket.execution.batch_no && (
          <span className="sandbox-meta">模拟批次 {ticket.execution.batch_no} · {ticket.execution.batch_status}</span>
        )}
        {!compact && ticket.formal_dag_untouched && <span className="sandbox-safe">正式 DAG 未被修改</span>}
      </header>

      <div className="sandbox-waterline">
        <span>阻塞水位：<strong>{ticket.blocking_watermark ?? '无'}</strong></span>
        <span>所在任务：<strong>{ticket.blocked_task ?? '无'}</strong></span>
        <span>可补跑段：<strong>{ticket.segments.filter((item) => item.in_backfill_scope).length}</strong></span>
        <span>待处理阻塞点：<strong>{ticket.blocking_points.filter((item) => !item.resolved).length}</strong></span>
      </div>

      {ticket.blocking_points.length > 0 && (
        <div className="sandbox-blocking">
          {ticket.blocking_points.map((point) => (
            <article key={point.blocking_id} className={`blocking-card ${point.resolved ? 'resolved' : 'open'}`}>
              <div className="blocking-line">
                <strong>{point.blocking_id} · {point.title}</strong>
                <span className={`tag tag-${point.type}`}>{point.type}</span>
                {point.resolved && <span className="tag tag-done">已处理</span>}
              </div>
              <p>{point.description}</p>
              {point.cycle_path && point.cycle_path.length > 0 && (
                <p className="mono">成环路径：{point.cycle_path.join(' → ')}</p>
              )}
              {point.resolution && (
                <p className="muted">
                  处理方式：{ACTION_LABEL[point.resolution.action] ?? point.resolution.action} → {point.resolution.target}
                  {point.resolution.remark ? `；备注：${point.resolution.remark}` : ''}（{point.resolution.operator}）
                </p>
              )}
              {!point.resolved && (
                <div className="blocking-actions">
                  {point.options.map((option) => (
                    <button
                      key={`${option.action}-${option.target}`}
                      className={`btn ${option.recommended ? 'primary' : ''}`}
                      type="button"
                      onClick={() => void resolveBlocking(ticket.ticket_id, point.blocking_id, option, onChanged)}
                    >
                      {option.label}
                    </button>
                  ))}
                  {point.options.length === 0 && <span className="muted">正式 DAG 自身问题，需调度管理员线下修复</span>}
                </div>
              )}
            </article>
          ))}
        </div>
      )}

      <h4 className="sandbox-subtitle">有序路径（含上游任务、已完成水位与待补跑分区）</h4>
      <table className="data-table sandbox-table">
        <thead>
          <tr>
            <th>边序段</th>
            <th>任务</th>
            <th>角色</th>
            <th>上游边</th>
            <th>已完成水位</th>
            <th>仍需补跑分区</th>
            <th>本段阻塞水位</th>
            <th>批次状态</th>
          </tr>
        </thead>
        <tbody>
          {ticket.segments.map((segment) => {
            const batchSegment = batchSegments.find((item) => item.seq === segment.seq)
            return (
              <tr key={segment.seq} className={segment.in_backfill_scope ? '' : 'sandbox-readonly'}>
                <td>{segment.seq}</td>
                <td>
                  <span className="mono">{segment.task_id}</span>
                  <span className="cell-sub">{segment.task_name} · {segment.layer}</span>
                </td>
                <td>{ROLE_LABEL[segment.role] ?? segment.role}</td>
                <td className="mono">
                  {segment.incoming_edges.length === 0 && '—'}
                  {segment.incoming_edges.map((edge) => (
                    <span key={edge.edge_id} className="edge-chip">
                      {edge.edge_id}
                      {edge.cycle_risk && <em className="risk">成环风险</em>}
                      {edge.missing_dependency && <em className="risk">缺失依赖</em>}
                      {edge.deferred && <em className="muted">已暂缓</em>}
                    </span>
                  ))}
                </td>
                <td>{segment.completed_watermark ?? '—'}</td>
                <td>
                  {segment.pending_partition_count === 0
                    ? '无（只读上游）'
                    : `${segment.pending_partition_count} 个：${segment.resume_from} ~ ${segment.pending_partitions[segment.pending_partitions.length - 1]}`}
                </td>
                <td>{segment.blocking_watermark ?? '—'}</td>
                <td>
                  {batchSegment
                    ? <span className={`tag status-${statusKey(batchSegment.status)}`}>{batchSegment.status}</span>
                    : <span className="muted">未确认</span>}
                </td>
              </tr>
            )
          })}
        </tbody>
      </table>

      <h4 className="sandbox-subtitle">边序（确认执行时按此顺序推进）</h4>
      <table className="data-table sandbox-table">
        <thead>
          <tr><th>边序</th><th>边号</th><th>上游 → 下游</th><th>来源</th><th>进入补跑</th><th>批次边状态</th></tr>
        </thead>
        <tbody>
          {ticket.edge_order.map((edge) => {
            const batchEdge = batchEdges.find((item) => item.edge_id === edge.edge_id)
            return (
              <tr key={edge.edge_id}>
                <td>{edge.order}</td>
                <td className="mono">
                  {edge.edge_id}
                  {edge.cycle_risk && <em className="risk">成环风险</em>}
                </td>
                <td className="mono">{edge.upstream_task} → {edge.downstream_task}</td>
                <td>{edge.source === 'candidate' ? '候选（待发布）' : '正式'}</td>
                <td>{edge.execution_included ? '是' : '否（只读上游）'}</td>
                <td>{batchEdge ? batchEdge.status : <span className="muted">未确认</span>}</td>
              </tr>
            )
          })}
          {ticket.edge_order.length === 0 && (
            <tr><td className="empty-state" colSpan={6}>当前路径没有可执行的依赖边</td></tr>
          )}
        </tbody>
      </table>

      {ticket.rejected_edges.length > 0 && (
        <details className="sandbox-rejected">
          <summary>未进入推演路径的边（{ticket.rejected_edges.length}）</summary>
          <ul>
            {ticket.rejected_edges.map((edge) => (
              <li key={edge.edge_id} className="mono">
                {edge.edge_id}（{edge.upstream_task} → {edge.downstream_task}）：{edge.reject_reason}
              </li>
            ))}
          </ul>
        </details>
      )}

      <div className="sandbox-foot">
        {!executed && (
          <button className="btn primary" type="button" onClick={() => void executeTicket(ticket.ticket_id, onChanged)}>
            确认执行（仅创建 {ticket.execution.batch_no} 模拟批次）
          </button>
        )}
        {ticket.scope_empty && <span className="muted">日期窗内根任务无待补跑分区，确认执行会被拦下。</span>}
        {executed && <span className="muted">已确认执行，沿用批次 {ticket.execution.batch_id}；再次确认不会重复创建。</span>}
        <span className="sandbox-hint">确认执行只推进模拟批次，不会修改正式依赖；正式依赖发布需单独确认。</span>
      </div>

      {batch && (
        <>
          <h4 className="sandbox-subtitle">模拟批次 {batch.batch_no} 推进结果（失败段停下，后续段保持未开始）</h4>
          <div className="sandbox-batch">
            {batch.segments.map((segment) => (
              <article key={segment.seq} className={`batch-segment batch-${statusKey(segment.status)}`}>
                <header>
                  <span>段 {segment.seq} · <span className="mono">{segment.task_id}</span> {segment.task_name}</span>
                  <span className={`tag status-${statusKey(segment.status)}`}>{segment.status}</span>
                </header>
                <div className="partition-list">
                  {segment.partitions.map((partition) => (
                    <span key={partition.partition_date} className={`partition status-${statusKey(partition.status)}`} title={partition.message}>
                      {partition.partition_date} · {partition.status}
                    </span>
                  ))}
                </div>
              </article>
            ))}
          </div>
        </>
      )}
    </section>
  )
}

async function resolveBlocking(
  ticketId: string,
  blockingId: string,
  option: { action: string; target: string; label: string },
  onChanged?: () => void,
) {
  try {
    await readJson(`/api/sandbox/tickets/${ticketId}/blocking-points/${blockingId}/resolve`, {
      method: 'POST',
      body: JSON.stringify({ action: option.action, target: option.target, operator: '值班管理员' }),
    })
  } catch (error) {
    window.alert(error instanceof Error ? error.message : '阻塞点处理失败')
    return
  }
  onChanged?.()
}

async function executeTicket(ticketId: string, onChanged?: () => void) {
  try {
    await readJson(`/api/sandbox/tickets/${ticketId}/execute`, {
      method: 'POST',
      body: JSON.stringify({ confirm: true, operator: '值班管理员' }),
    })
  } catch (error) {
    window.alert(error instanceof Error ? error.message : '确认执行失败')
    return
  }
  onChanged?.()
}

export function SandboxCreateForm({
  onCreated,
  defaultRoot,
  defaultStart,
  defaultEnd,
}: {
  onCreated?: () => void
  defaultRoot: string
  defaultStart: string
  defaultEnd: string
}) {
  const [rootTask, setRootTask] = useState(defaultRoot)
  const [startDate, setStartDate] = useState(defaultStart)
  const [endDate, setEndDate] = useState(defaultEnd)
  const [message, setMessage] = useState('')

  useEffect(() => {
    setRootTask(defaultRoot)
    setStartDate(defaultStart)
    setEndDate(defaultEnd)
  }, [defaultRoot, defaultStart, defaultEnd])

  async function submit(event: React.FormEvent) {
    event.preventDefault()
    setMessage('')
    try {
      const result = await readJson<{ ok: boolean; message: string }>('/api/sandbox/tickets', {
        method: 'POST',
        body: JSON.stringify({
          root_task: rootTask,
          start_date: startDate,
          end_date: endDate,
          include_candidate_edges: true,
          operator: '值班管理员',
        }),
      })
      setMessage(result.message)
      onCreated?.()
    } catch (error) {
      setMessage(error instanceof Error ? error.message : '路径票生成失败')
    }
  }

  return (
    <form className="filter-bar sandbox-create" onSubmit={(event) => void submit(event)}>
      <label className="filter-item">
        <span>根任务</span>
        <input value={rootTask} onChange={(event) => setRootTask(event.target.value)} placeholder="如 TASK-0002" />
      </label>
      <label className="filter-item">
        <span>分区开始日期</span>
        <input type="date" value={startDate} onChange={(event) => setStartDate(event.target.value)} />
      </label>
      <label className="filter-item">
        <span>分区结束日期</span>
        <input type="date" value={endDate} onChange={(event) => setEndDate(event.target.value)} />
      </label>
      <button className="btn primary" type="submit">生成 / 沿用路径票</button>
      {message && <span className="sandbox-create-msg">{message}</span>}
    </form>
  )
}

export function useSandboxTicket() {
  const [ticket, setTicket] = useState<SandboxTicket | null>(null)
  const [batch, setBatch] = useState<BatchDetail | null>(null)
  const [context, setContext] = useState<SandboxContext | null>(null)
  const [archive, setArchive] = useState<TicketArchive[]>([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState('')

  const reload = useCallback(async () => {
    setError('')
    try {
      const [contextPayload, ticketPayload, historyPayload] = await Promise.all([
        readJson<SandboxContext>('/api/sandbox/context'),
        readJson<{ ticket: SandboxTicket | null }>('/api/sandbox/tickets/latest'),
        readJson<{ ticket_archives: TicketArchive[] }>('/api/sandbox/history'),
      ])
      setContext(contextPayload)
      setTicket(ticketPayload.ticket)
      setArchive(historyPayload.ticket_archives)
      const batchNo = ticketPayload.ticket?.execution.batch_id
      if (batchNo) {
        const batchPayload = await readJson<BatchDetail>(`/api/sandbox/batches/${batchNo}`).catch(() => null)
        setBatch(batchPayload)
      } else {
        setBatch(null)
      }
    } catch (catchError) {
      setError(catchError instanceof Error ? catchError.message : '依赖沙盘读取失败')
    } finally {
      setLoading(false)
    }
  }, [])

  useEffect(() => {
    void reload()
  }, [reload])

  const scenario = useMemo(
    () => ({
      root: context?.scenario.root_task ?? 'TASK-0002',
      start: context?.scenario.start_date ?? '2026-09-25',
      end: context?.scenario.end_date ?? '2026-10-05',
      batchNo: context?.scenario.batch_no ?? 'BACKFILL-0002',
      dagVersion: context?.dag_version ?? 'v3',
      candidates: context?.candidate_edges ?? [],
    }),
    [context],
  )

  return { ticket, batch, context, archive, scenario, loading, error, reload }
}

export function SandboxPublishPanel({ onPublished }: { onPublished?: () => void }) {
  const [message, setMessage] = useState('')
  const [note, setNote] = useState('')
  const [edgeIds, setEdgeIds] = useState('')

  async function publish() {
    setMessage('')
    try {
      const payload = await readJson<{ ok: boolean; message: string; dag_version: string }>(
        '/api/sandbox/dependencies/publish',
        {
          method: 'POST',
          body: JSON.stringify({
            confirm: true,
            edge_ids: edgeIds.trim() ? edgeIds.split(',').map((item) => item.trim()).filter(Boolean) : null,
            note,
            operator: '调度管理员',
          }),
        },
      )
      setMessage(`${payload.message}（当前版本 ${payload.dag_version}）`)
      onPublished?.()
    } catch (error) {
      setMessage(error instanceof Error ? error.message : '正式依赖发布失败')
    }
  }

  return (
    <div className="sandbox-publish">
      <label className="filter-item">
        <span>发布备注</span>
        <input value={note} onChange={(event) => setNote(event.target.value)} placeholder="正式依赖发布说明" />
      </label>
      <label className="filter-item">
        <span>候选边（逗号分隔，留空表示全部待发布）</span>
        <input value={edgeIds} onChange={(event) => setEdgeIds(event.target.value)} placeholder="如 CAND-0001" />
      </label>
      <button className="btn" type="button" onClick={() => void publish()}>单独确认并发布正式依赖</button>
      {message && <span className="sandbox-create-msg">{message}</span>}
    </div>
  )
}

function ticketStatusKey(status: string): string {
  if (status === '阻塞待处理') return 'blocked'
  if (status === '可执行') return 'ready'
  if (status === '执行受阻') return 'paused'
  if (status === '已执行') return 'done'
  return 'pending'
}

function statusKey(status: string): string {
  if (status === '成功' || status === '已完成' || status === '已通过') return 'done'
  if (status === '失败' || status === '被阻塞' || status.includes('被阻塞')) return 'blocked'
  if (status === '未开始') return 'pending'
  if (status === '运行中') return 'running'
  return 'pending'
}
