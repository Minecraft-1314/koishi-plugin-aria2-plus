import { Context, Schema, Service } from 'koishi'

export interface Aria2File {
  index: string
  path: string
  length: string
  completedLength: string
  selected: string
  uris: Array<{ uri: string; status: string }>
}

export interface Aria2Status {
  gid: string
  status: 'active' | 'waiting' | 'paused' | 'error' | 'complete' | 'removed'
  totalLength: string
  completedLength: string
  downloadSpeed: string
  uploadSpeed: string
  infoHash?: string
  numSeeders?: string
  seeder?: string
  pieceLength?: string
  numPieces?: string
  connections?: string
  errorCode?: string
  errorMessage?: string
  followedBy?: string[]
  following?: string
  belongsTo?: string
  dir?: string
  files?: Aria2File[]
  bittorrent?: Record<string, any>
  [key: string]: any
}

export interface GlobalStat {
  downloadSpeed: string
  uploadSpeed: string
  numActive: string
  numWaiting: string
  numStopped: string
  numStoppedTotal: string
}

export interface SessionInfo {
  sessionId: string
}

export interface Aria2Event {
  gid: string
  event: 'start' | 'pause' | 'stop' | 'complete' | 'error' | 'btComplete'
  [key: string]: any
}

declare module 'koishi' {
  interface Context {
    aria2: Aria2
  }
  interface Events {
    'aria2/event'(event: Aria2Event): void
  }
}

class Aria2 extends Service {
  static [Service.provide] = 'aria2'
  static inject = ['http']
  static filter = false

  private secret = ''
  private endpoint = ''
  private retries = 2
  private pollingTimer: NodeJS.Timeout | null = null
  private knownGids = new Set<string>()
  private wsClient: any = null

  constructor(ctx: Context, public config: Aria2.Config) {
    super(ctx, 'aria2')
  }

  async start() {
    const { host, port, path, secure, secret, retry, events } = this.config
    this.secret = secret ?? ''
    this.endpoint = `${secure ? 'https' : 'http'}://${host}:${port}${path}`
    this.retries = retry ?? 2

    try {
      const version = await this.call('aria2.getVersion')
      this.logger.info('aria2 连接成功 (v%s)', version.version)
    } catch (e: any) {
      this.logger.warn('aria2 连接测试失败: %s', e.message)
    }

    if (events) {
      if (!(await this.tryWebSocket())) {
        this.startPolling()
      }
    }
  }

  async stop() {
    if (this.pollingTimer) {
      clearInterval(this.pollingTimer)
      this.pollingTimer = null
    }
    if (this.wsClient) {
      this.wsClient.close()
      this.wsClient = null
    }
  }

  private async tryWebSocket(): Promise<boolean> {
    try {
      const WS = (await import('ws')).default
      const url = this.endpoint.replace(/^http/, 'ws')
      const ws = new WS(url)
      this.wsClient = ws

      ws.on('open', () => {
        if (this.config.debug) this.logger.debug('WebSocket 已连接')
      })

      ws.on('message', (data: string) => {
        this.handleWebSocketMessage(data)
      })

      ws.on('close', (code: number, reason: string) => {
        if (this.config.debug) this.logger.debug('WebSocket 断开 (code: %d, reason: %s)，回退到轮询', code, reason)
        this.cleanupWs()
        this.startPolling()
      })

      ws.on('error', (err: Error) => {
        if (this.config.debug) this.logger.debug('WebSocket 错误: %s，回退到轮询', err.message)
        this.cleanupWs()
        this.startPolling()
      })
      return true
    } catch {
      if (this.config.debug) this.logger.debug('无法加载 ws 模块，将使用轮询')
      return false
    }
  }

  private cleanupWs() {
    if (this.wsClient) {
      this.wsClient.close()
      this.wsClient = null
    }
  }

  private handleWebSocketMessage(data: string) {
    try {
      const msg = JSON.parse(data)
      const eventMap: Record<string, Aria2Event['event']> = {
        'aria2.onDownloadStart': 'start',
        'aria2.onDownloadPause': 'pause',
        'aria2.onDownloadStop': 'stop',
        'aria2.onDownloadComplete': 'complete',
        'aria2.onDownloadError': 'error',
        'aria2.onBtDownloadComplete': 'btComplete',
      }
      const eventType = eventMap[msg.method]
      if (eventType && msg.params?.length) {
        const event: Aria2Event = {
          gid: msg.params[0].gid,
          event: eventType,
          ...msg.params[0],
        }
        this.ctx.emit('aria2/event', event)
      }
    } catch {}
  }

  private startPolling() {
    if (this.pollingTimer) clearInterval(this.pollingTimer)
    this.logger.info('启动轮询模式 (每 3 秒)')
    this.pollingTimer = setInterval(async () => {
      try {
        const [active, waiting, stopped] = await Promise.all([
          this.tellActive(['gid', 'status']),
          this.tellWaiting(0, 100, ['gid', 'status']),
          this.tellStopped(0, 100, ['gid', 'status']),
        ])
        const all = [...active, ...waiting, ...stopped]
        for (const task of all) {
          if (!this.knownGids.has(task.gid)) {
            if (this.knownGids.size >= 1000) {
              const first = this.knownGids.values().next().value
              if (first) this.knownGids.delete(first)
            }
            this.knownGids.add(task.gid)
            this.ctx.emit('aria2/event', { gid: task.gid, event: 'start' })
          }
        }
        for (const gid of this.knownGids) {
          if (!all.find(t => t.gid === gid)) {
            this.knownGids.delete(gid)
            this.ctx.emit('aria2/event', { gid, event: 'complete' })
          }
        }
      } catch (e: any) {
        if (this.config.debug) this.logger.debug('轮询出错: %s', e.message)
      }
    }, 3000)
  }

  private async call(method: string, params?: any[], retryCount = 0): Promise<any> {
    const payload = {
      jsonrpc: '2.0',
      id: `k-ar2-${Date.now()}-${Math.random().toString(36).slice(2)}`,
      method,
      params: this.secret ? [`token:${this.secret}`, ...(params || [])] : (params || []),
    }

    if (this.config.debug) this.logger.debug('RPC -> %s %o', method, payload)

    try {
      const response = await this.ctx.http.post(this.endpoint, payload, {
        headers: { 'Content-Type': 'application/json' },
        timeout: this.config.timeout ?? 10000,
      })
      const data = typeof response === 'string' ? JSON.parse(response) : response
      if (this.config.debug) this.logger.debug('RPC <- %o', data)
      if (data.error) {
        const err = new Error(`[aria2 ${data.error.code}] ${data.error.message}`)
        ;(err as any).code = data.error.code
        throw err
      }
      return data.result
    } catch (err: any) {
      if (this.config.debug) this.logger.debug('RPC 失败: %s (code: %s)', err.message, err.code)
      const noRetryMethods = ['aria2.shutdown', 'aria2.forceShutdown']
      if (
        retryCount < this.retries &&
        !noRetryMethods.includes(method) &&
        (err.code === 'ECONNREFUSED' || err.code === 'ECONNRESET' ||
         err.code === 'ETIMEDOUT' || err.name === 'TimeoutError')
      ) {
        this.logger.warn('重试 (%d/%d)', retryCount + 1, this.retries)
        return this.call(method, params, retryCount + 1)
      }
      throw err
    }
  }

  async multicall(methods: Array<{ methodName: string; params: any[] }>) {
    const calls = methods.map(({ methodName, params }) => ({
      methodName,
      params: this.secret ? [`token:${this.secret}`, ...params] : params,
    }))
    return this.call('system.multicall', [calls])
  }

  async addUri(uris: string[], options?: Record<string, any>) { return this.call('aria2.addUri', [uris, options || {}]) }
  async addTorrent(torrent: string, uris?: string[], options?: Record<string, any>) { return this.call('aria2.addTorrent', [torrent, uris || [], options || {}]) }
  async addMetalink(metalink: string, options?: Record<string, any>) { return this.call('aria2.addMetalink', [metalink, options || {}]) }
  async pause(gid: string) { return this.call('aria2.pause', [gid]) }
  async pauseAll() { return this.call('aria2.pauseAll') }
  async forcePause(gid: string) { return this.call('aria2.forcePause', [gid]) }
  async forcePauseAll() { return this.call('aria2.forcePauseAll') }
  async unpause(gid: string) { return this.call('aria2.unpause', [gid]) }
  async unpauseAll() { return this.call('aria2.unpauseAll') }
  async remove(gid: string) { return this.call('aria2.remove', [gid]) }
  async forceRemove(gid: string) { return this.call('aria2.forceRemove', [gid]) }
  async changeOption(gid: string, options: Record<string, any>) { return this.call('aria2.changeOption', [gid, options]) }
  async changeUri(gid: string, fileIndex: number, delUris: string[], addUris: string[]) { return this.call('aria2.changeUri', [gid, fileIndex, delUris, addUris]) }
  async getOption(gid: string) { return this.call('aria2.getOption', [gid]) }
  async changePosition(gid: string, pos: number, how: 'POS_SET' | 'POS_CUR' | 'POS_END') { return this.call('aria2.changePosition', [gid, pos, how]) }

  async tellStatus(gid: string, keys?: string[]): Promise<Aria2Status> { return this.call('aria2.tellStatus', [gid, keys]) }
  async tellActive(keys?: string[]): Promise<Aria2Status[]> { return this.call('aria2.tellActive', [keys]) }
  async tellWaiting(offset = 0, num = 1000, keys?: string[]): Promise<Aria2Status[]> { return this.call('aria2.tellWaiting', [offset, num, keys]) }
  async tellStopped(offset = 0, num = 1000, keys?: string[]): Promise<Aria2Status[]> { return this.call('aria2.tellStopped', [offset, num, keys]) }
  async tellAll(keys?: string[]) {
    const [active, waiting, stopped] = await this.multicall([
      { methodName: 'aria2.tellActive', params: [keys] },
      { methodName: 'aria2.tellWaiting', params: [0, 1000, keys] },
      { methodName: 'aria2.tellStopped', params: [0, 1000, keys] },
    ])
    return { active, waiting, stopped }
  }
  async getFiles(gid: string): Promise<Aria2File[]> { return this.call('aria2.getFiles', [gid]) }
  async getPeers(gid: string) { return this.call('aria2.getPeers', [gid]) }
  async getServers(gid: string) { return this.call('aria2.getServers', [gid]) }

  async getGlobalStat(): Promise<GlobalStat> { return this.call('aria2.getGlobalStat') }
  async getGlobalOption() { return this.call('aria2.getGlobalOption') }
  async changeGlobalOption(options: Record<string, any>) { return this.call('aria2.changeGlobalOption', [options]) }
  async purgeDownloadResult() { return this.call('aria2.purgeDownloadResult') }
  async removeDownloadResult(gid: string) { return this.call('aria2.removeDownloadResult', [gid]) }
  async getSessionInfo(): Promise<SessionInfo> { return this.call('aria2.getSessionInfo') }
  async saveSession() { return this.call('aria2.saveSession') }
  async shutdown() { return this.call('aria2.shutdown') }
  async forceShutdown() { return this.call('aria2.forceShutdown') }
  async getVersion(): Promise<{ version: string; enabledFeatures: string[] }> { return this.call('aria2.getVersion') }
}

namespace Aria2 {
  export interface Config {
    host: string
    port: number
    secure: boolean
    path: string
    secret?: string
    timeout?: number
    retry?: number
    events?: boolean
    debug?: boolean
  }

  export const Config: Schema<Config> = Schema.object({
    host: Schema.string().description('aria2 RPC 服务地址。').default('localhost'),
    port: Schema.natural().max(65535).description('aria2 RPC 服务端口。').default(6800),
    secure: Schema.boolean().description('是否使用 HTTPS 连接。').default(false),
    path: Schema.string().pattern(/^\//).description('RPC 接口路径。').default('/jsonrpc'),
    secret: Schema.string().description('RPC 密钥 (rpc-secret)。').default(''),
    timeout: Schema.number().description('单次请求超时时间 (毫秒)。').default(10000),
    retry: Schema.natural().description('网络错误重试次数。').default(2),
    events: Schema.boolean().description('启用下载事件通知（自动尝试 WebSocket，不可用时回退到轮询）。').default(false),
    debug: Schema.boolean().description('开启调试日志。').default(false),
  })
}

export default Aria2