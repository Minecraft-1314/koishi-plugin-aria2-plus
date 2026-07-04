import { Context, Schema, Service } from 'koishi'
import type WebSocket from 'ws'

// ─── 类型定义 ────────────────────────────────────────────

/**
 * aria2 返回的文件信息
 */
export interface Aria2File {
  /** 文件在任务中的索引 */
  index: string
  /** 文件完整路径 */
  path: string
  /** 文件总大小（字节） */
  length: string
  /** 已完成大小（字节） */
  completedLength: string
  /** 文件是否被选中下载 */
  selected: string
  /** 该文件的下载链接及状态 */
  uris: Array<{ uri: string; status: string }>
}

/**
 * aria2 任务状态
 */
export interface Aria2Status {
  /** 任务唯一标识 */
  gid: string
  /** 任务状态：active（活动）、waiting（等待）、paused（暂停）、error（错误）、complete（完成）、removed（已移除） */
  status: 'active' | 'waiting' | 'paused' | 'error' | 'complete' | 'removed'
  /** 总大小（字节） */
  totalLength: string
  /** 已完成大小（字节） */
  completedLength: string
  /** 下载速度（字节/秒） */
  downloadSpeed: string
  /** 上传速度（字节/秒） */
  uploadSpeed: string
  /** BT 信息哈希 */
  infoHash?: string
  /** 种子数量 */
  numSeeders?: string
  /** 是否做种中 */
  seeder?: string
  /** 区块大小 */
  pieceLength?: string
  /** 区块总数 */
  numPieces?: string
  /** 连接数 */
  connections?: string
  /** 错误代码 */
  errorCode?: string
  /** 错误消息 */
  errorMessage?: string
  /** 后续任务 GID 列表（由该任务触发） */
  followedBy?: string[]
  /** 前驱任务 GID（触发此任务的任务） */
  following?: string
  /** 所属 BT 任务 GID */
  belongsTo?: string
  /** 下载目录 */
  dir: string
  /** 包含的文件列表 */
  files: Aria2File[]
  /** BT 相关信息 */
  bittorrent?: Record<string, any>
  /** 其他未知字段 */
  [key: string]: any
}

/**
 * 全局统计信息
 */
export interface GlobalStat {
  /** 全局下载速度（字节/秒） */
  downloadSpeed: string
  /** 全局上传速度（字节/秒） */
  uploadSpeed: string
  /** 活动任务数 */
  numActive: string
  /** 等待任务数 */
  numWaiting: string
  /** 当前会话已停止任务数 */
  numStopped: string
  /** 累计停止任务数（包含历史） */
  numStoppedTotal: string
}

/**
 * 会话信息
 */
export interface SessionInfo {
  /** 会话 ID */
  sessionId: string
}

/**
 * aria2 下载事件
 */
export interface Aria2Event {
  /** 任务 GID */
  gid: string
  /** 事件类型：start（开始）、pause（暂停）、stop（停止）、complete（完成）、error（错误）、btComplete（BT 完整下载） */
  event: 'start' | 'pause' | 'stop' | 'complete' | 'error' | 'btComplete'
  /** 事件附加的其他数据 */
  [key: string]: any
}

// 扩展 Koishi 的 Context 和 Events，使其他插件能够通过 ctx.aria2 访问本服务，并监听 aria2/event 事件
declare module 'koishi' {
  interface Context {
    aria2: Aria2
  }
  interface Events {
    'aria2/event'(event: Aria2Event): void
  }
}

// ─── 服务主体 ────────────────────────────────────────────

/**
 * aria2 下载服务
 * 通过 HTTP JSON-RPC 与 aria2 守护进程通信，并提供可选的 WebSocket 事件推送。
 */
class Aria2 extends Service {
  /** 服务标识 */
  static [Service.provide] = 'aria2'
  /** 依赖的服务：需要 http 服务来进行网络请求 */
  static inject = ['http']

  /** RPC 密钥，从配置中读取 */
  private secret: string = ''
  /** RPC 端点 URL */
  private endpoint: string = ''
  /** WebSocket 客户端实例（连接 aria2 的事件推送） */
  private wsClient: WebSocket | null = null
  /** 重连定时器 */
  private reconnectTimer: NodeJS.Timeout | null = null
  /** 网络错误重试次数 */
  private retries: number = 2
  /** 标记服务是否已停止，用于阻止重连和操作 */
  private stopped = false

  constructor(ctx: Context, public config: Aria2.Config) {
    super(ctx, 'aria2')
  }

  /**
   * 服务启动：测试连接、初始化参数，并在需要时连接 WebSocket 事件流。
   */
  async start() {
    const { host, port, path, secure, secret, retry, events } = this.config
    // 确保 secret 不为 undefined
    this.secret = secret || ''
    // 拼接完整的 JSON-RPC 端点
    this.endpoint = `${secure ? 'https' : 'http'}://${host}:${port}${path}`
    this.retries = retry ?? 2
    this.stopped = false

    // 测试 RPC 连接是否正常
    try {
      const version = await this.call('aria2.getVersion')
      this.logger.info('aria2 连接成功 (v%s)', version.version)
    } catch (e) {
      this.logger.warn('aria2 连接测试失败，请检查配置或确保服务已启动')
    }

    // 如果启用了事件通知，则尝试连接 aria2 的 WebSocket
    if (events) {
      await this.connectAria2WebSocket()
    }
  }

  /**
   * 服务停止：清理 WebSocket 连接和定时器。
   */
  async stop() {
    this.stopped = true
    if (this.wsClient) {
      this.wsClient.close()
      this.wsClient = null
    }
    if (this.reconnectTimer) {
      clearTimeout(this.reconnectTimer)
      this.reconnectTimer = null
    }
  }

  /**
   * 内部连接 aria2 的 WebSocket，并将下载事件转化为 Koishi 事件。
   * 连接断开后会自动重试（5秒后）。
   */
  private async connectAria2WebSocket() {
    try {
      // 动态导入 ws 模块，避免强制依赖
      const { default: WS } = await import('ws')
      // WebSocket 端点由 HTTP 端点转换而来（http -> ws）
      const url = this.endpoint.replace(/^http/, 'ws')

      // 连接函数，用于首次连接和重连
      const connect = () => {
        if (this.stopped) return
        this.wsClient = new WS(url)

        this.wsClient.on('open', () => {
          this.logger.debug('aria2 WebSocket 已连接')
        })

        this.wsClient.on('message', (data: string) => {
          try {
            const msg = JSON.parse(data)
            // 将 aria2 的原始事件映射为标准事件类型
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
              // 通过 Koishi 事件系统广播
              this.ctx.emit('aria2/event', event)
            }
          } catch {
            // 忽略解析错误
          }
        })

        this.wsClient.on('close', () => {
          if (this.stopped) return
          this.logger.debug('aria2 WebSocket 断开，5 秒后重连')
          this.reconnectTimer = setTimeout(connect, 5000)
        })

        this.wsClient.on('error', (err) => {
          this.logger.debug('aria2 WebSocket 错误: %s', err.message)
        })
      }

      connect()
    } catch (e) {
      this.logger.error('无法加载 ws 模块，事件功能不可用。请安装 ws 依赖: npm install ws')
    }
  }

  /**
   * 发送 JSON-RPC 请求，并自动处理错误与重试（仅网络/超时错误）。
   * @param method - aria2 的 RPC 方法名
   * @param params - 方法参数（不包含 secret token）
   * @param retryCount - 当前已重试次数（内部使用）
   * @returns 返回结果中的 result 字段
   */
  private async call(method: string, params?: any[], retryCount = 0): Promise<any> {
    const payload = {
      jsonrpc: '2.0',
      // 生成唯一请求 ID
      id: `koishi-aria2-${Date.now()}-${Math.random().toString(36).slice(2)}`,
      method,
      // 如果有密钥，自动在参数最前面加上 token
      params: this.secret ? [`token:${this.secret}`, ...(params || [])] : (params || []),
    }

    // 调试模式下打印请求内容
    if (this.config.debug) {
      this.logger.debug('aria2 RPC 请求: %s %o', method, payload)
    }

    try {
      const response = await this.ctx.http.post(this.endpoint, payload, {
        headers: { 'Content-Type': 'application/json' },
        timeout: this.config.timeout ?? 10000,
      })

      // 处理可能的字符串响应（部分 HTTP 客户端可能会自动解析 JSON）
      const data: any = typeof response === 'string' ? JSON.parse(response) : response

      if (this.config.debug) {
        this.logger.debug('aria2 RPC 响应: %o', data)
      }

      // 如果返回了错误对象，直接抛出
      if (data.error) {
        throw new Error(`[aria2 ${data.error.code}] ${data.error.message}`)
      }
      return data.result
    } catch (err) {
      const code = (err as any).code
      const name = (err as any).name
      // 只对网络连接错误或超时进行重试，避免业务错误也被重试
      if (
        retryCount < this.retries &&
        (code === 'ECONNREFUSED' || code === 'ECONNRESET' ||
         code === 'ETIMEDOUT' || name === 'TimeoutError')
      ) {
        this.logger.warn('调用失败，重试中... (%d/%d)', retryCount + 1, this.retries)
        return this.call(method, params, retryCount + 1)
      }
      throw err
    }
  }

  /**
   * 批量调用多个 RPC 方法（system.multicall），减少请求次数。
   * @param methods - 要调用的方法名和参数列表
   * @returns 按调用顺序返回的结果数组
   */
  async multicall(
    methods: Array<{ methodName: string; params: any[] }>,
  ): Promise<any[]> {
    const calls = methods.map(({ methodName, params }) => ({
      methodName,
      // 同样需要处理 token
      params: this.secret ? [`token:${this.secret}`, ...params] : params,
    }))
    return this.call('system.multicall', [calls])
  }

  // ════════════════════════════════════════════════════════
  // 以下为 aria2 各项具体功能的方法封装
  // 所有方法均返回 Promise，并保留原始返回类型
  // ════════════════════════════════════════════════════════

  // ─── 任务管理 ──────────────────────────────────────────

  /** 添加 URI 下载任务 */
  async addUri(uris: string[], options?: Record<string, any>): Promise<string> {
    return this.call('aria2.addUri', [uris, options || {}])
  }

  /** 添加种子下载（种子文件为 base64 编码） */
  async addTorrent(torrent: string, uris?: string[], options?: Record<string, any>): Promise<string> {
    return this.call('aria2.addTorrent', [torrent, uris || [], options || {}])
  }

  /** 添加 Metalink 下载 */
  async addMetalink(metalink: string, options?: Record<string, any>): Promise<string[]> {
    return this.call('aria2.addMetalink', [metalink, options || {}])
  }

  /** 暂停指定任务 */
  async pause(gid: string): Promise<string> {
    return this.call('aria2.pause', [gid])
  }

  /** 暂停所有任务 */
  async pauseAll(): Promise<string> {
    return this.call('aria2.pauseAll')
  }

  /** 强制暂停任务（即使连接断开） */
  async forcePause(gid: string): Promise<string> {
    return this.call('aria2.forcePause', [gid])
  }

  /** 强制暂停所有任务 */
  async forcePauseAll(): Promise<string> {
    return this.call('aria2.forcePauseAll')
  }

  /** 继续已暂停的任务 */
  async unpause(gid: string): Promise<string> {
    return this.call('aria2.unpause', [gid])
  }

  /** 继续所有暂停的任务 */
  async unpauseAll(): Promise<string> {
    return this.call('aria2.unpauseAll')
  }

  /** 移除任务（保留文件） */
  async remove(gid: string): Promise<string> {
    return this.call('aria2.remove', [gid])
  }

  /** 强制移除任务（即使正在下载也立即停止并清理） */
  async forceRemove(gid: string): Promise<string> {
    return this.call('aria2.forceRemove', [gid])
  }

  /** 修改任务选项 */
  async changeOption(gid: string, options: Record<string, any>): Promise<string> {
    return this.call('aria2.changeOption', [gid, options])
  }

  /** 替换任务中指定文件的下载链接 */
  async changeUri(gid: string, fileIndex: number, delUris: string[], addUris: string[]): Promise<string> {
    return this.call('aria2.changeUri', [gid, fileIndex, delUris, addUris])
  }

  /** 获取任务的当前选项 */
  async getOption(gid: string): Promise<Record<string, any>> {
    return this.call('aria2.getOption', [gid])
  }

  /** 调整任务在队列中的位置 */
  async changePosition(gid: string, pos: number, how: 'POS_SET' | 'POS_CUR' | 'POS_END'): Promise<number> {
    return this.call('aria2.changePosition', [gid, pos, how])
  }

  // ─── 状态查询 ──────────────────────────────────────────

  /** 查询任务详细信息 */
  async tellStatus(gid: string, keys?: string[]): Promise<Aria2Status> {
    return this.call('aria2.tellStatus', [gid, keys])
  }

  /** 获取当前所有活动任务 */
  async tellActive(keys?: string[]): Promise<Aria2Status[]> {
    return this.call('aria2.tellActive', [keys])
  }

  /** 获取等待队列中的任务 */
  async tellWaiting(offset?: number, num?: number, keys?: string[]): Promise<Aria2Status[]> {
    return this.call('aria2.tellWaiting', [offset ?? 0, num ?? 1000, keys])
  }

  /** 获取已停止的任务（包括已完成、错误、移除） */
  async tellStopped(offset?: number, num?: number, keys?: string[]): Promise<Aria2Status[]> {
    return this.call('aria2.tellStopped', [offset ?? 0, num ?? 1000, keys])
  }

  /** 一次性获取活动、等待、停止三类任务 */
  async tellAll(keys?: string[]): Promise<{
    active: Aria2Status[]
    waiting: Aria2Status[]
    stopped: Aria2Status[]
  }> {
    const [active, waiting, stopped] = await this.multicall([
      { methodName: 'aria2.tellActive', params: [keys] },
      { methodName: 'aria2.tellWaiting', params: [0, 1000, keys] },
      { methodName: 'aria2.tellStopped', params: [0, 1000, keys] },
    ])
    return { active, waiting, stopped }
  }

  /** 获取任务中的文件列表 */
  async getFiles(gid: string): Promise<Aria2File[]> {
    return this.call('aria2.getFiles', [gid])
  }

  /** 获取 BT 任务的对等点信息 */
  async getPeers(gid: string): Promise<any[]> {
    return this.call('aria2.getPeers', [gid])
  }

  /** 获取 BT 任务的服务器信息 */
  async getServers(gid: string): Promise<any[]> {
    return this.call('aria2.getServers', [gid])
  }

  // ─── 全局操作 ──────────────────────────────────────────

  /** 获取全局下载/上传速度及任务数量统计 */
  async getGlobalStat(): Promise<GlobalStat> {
    return this.call('aria2.getGlobalStat')
  }

  /** 获取全局选项 */
  async getGlobalOption(): Promise<Record<string, any>> {
    return this.call('aria2.getGlobalOption')
  }

  /** 修改全局选项 */
  async changeGlobalOption(options: Record<string, any>): Promise<string> {
    return this.call('aria2.changeGlobalOption', [options])
  }

  /** 清除已完成/错误/已删除任务的内存记录 */
  async purgeDownloadResult(): Promise<string> {
    return this.call('aria2.purgeDownloadResult')
  }

  /** 移除指定任务的内存记录（不影响已下载文件） */
  async removeDownloadResult(gid: string): Promise<string> {
    return this.call('aria2.removeDownloadResult', [gid])
  }

  /** 获取当前会话信息 */
  async getSessionInfo(): Promise<SessionInfo> {
    return this.call('aria2.getSessionInfo')
  }

  /** 强制将会话保存到磁盘 */
  async saveSession(): Promise<string> {
    return this.call('aria2.saveSession')
  }

  /** 正常关闭 aria2 进程 */
  async shutdown(): Promise<string> {
    return this.call('aria2.shutdown')
  }

  /** 强制关闭 aria2 进程 */
  async forceShutdown(): Promise<string> {
    return this.call('aria2.forceShutdown')
  }

  /** 获取 aria2 版本及启用的功能列表 */
  async getVersion(): Promise<{ version: string; enabledFeatures: string[] }> {
    return this.call('aria2.getVersion')
  }
}

// ─── 配置 Schema ─────────────────────────────────────────
namespace Aria2 {
  export interface Config {
    /** aria2 RPC 服务地址 */
    host: string
    /** aria2 RPC 服务端口 */
    port: number
    /** 是否使用 HTTPS 连接 */
    secure: boolean
    /** RPC 接口路径 */
    path: string
    /** RPC 密钥（对应 aria2 的 rpc-secret） */
    secret?: string
    /** 单次请求超时时间（毫秒） */
    timeout?: number
    /** 网络错误重试次数 */
    retry?: number
    /** 是否启用 WebSocket 实时事件通知（需要安装 ws） */
    events?: boolean
    /** 是否开启调试日志（打印请求与响应） */
    debug?: boolean
  }

  /** 配置项的 Schema 定义，用于 Koishi 配置页面 */
  export const Config: Schema<Config> = Schema.object({
    host: Schema.string()
      .description('aria2 RPC 服务地址。')
      .default('localhost'),
    port: Schema.natural()
      .max(65535)
      .description('aria2 RPC 服务端口。')
      .default(6800),
    secure: Schema.boolean()
      .description('是否使用 HTTPS 连接。')
      .default(false),
    path: Schema.string()
      .pattern(/^\//)
      .description('RPC 接口路径，需与 aria2 配置文件中的 rpc-listen-port 等对应。')
      .default('/jsonrpc'),
    secret: Schema.string()
      .description('RPC 密钥（对应 aria2 的 rpc-secret）。')
      .default(''),
    timeout: Schema.number()
      .description('单次请求的超时时间（毫秒）。')
      .default(10000),
    retry: Schema.natural()
      .description('网络错误时的重试次数。')
      .default(2),
    events: Schema.boolean()
      .description('是否启用 WebSocket 实时事件通知。开启后可通过 `ctx.on("aria2/event")` 监听下载事件。需要安装 ws 依赖。')
      .default(false),
    debug: Schema.boolean()
      .description('是否开启调试日志，将打印完整的请求与响应数据。')
      .default(false),
  })
}

export default Aria2