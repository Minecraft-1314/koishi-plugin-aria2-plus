# koishi-plugin-aria2

## 项目介绍

### 中文
这是一个为 Koishi 机器人框架开发的 **aria2 下载服务插件**，通过 HTTP JSON-RPC 与 aria2 守护进程通信，并支持可选的 WebSocket 实时事件推送。插件完全封装了 aria2 的所有核心 JSON-RPC 方法。

### English
This is an **aria2 download service plugin** developed for the Koishi bot framework. It communicates with the aria2 daemon via HTTP JSON-RPC and supports optional WebSocket real-time event push. The plugin fully encapsulates all of aria2's core JSON-RPC methods.

## 安装

```bash
npm install koishi-plugin-aria2
```

**前置要求：**  
必须先在服务器上安装并启动 aria2 守护进程，并启用 RPC 模式。  
示例启动命令：
```bash
aria2c --enable-rpc --rpc-listen-all=true --rpc-allow-origin-all --rpc-secret=你的密钥
```

## 配置项说明

| 配置项 | 类型 | 默认值 | 说明 |
|--------|------|--------|------|
| `host` | string | `localhost` | aria2 RPC 服务地址 |
| `port` | number | `6800` | aria2 RPC 服务端口 |
| `secure` | boolean | `false` | 是否使用 HTTPS 连接 |
| `path` | string | `/jsonrpc` | RPC 接口路径（需与 aria2.conf 中一致） |
| `secret` | string | `''` | RPC 密钥（对应 aria2 的 `rpc-secret`） |
| `timeout` | number | `10000` | 单次请求的超时时间（毫秒） |
| `retry` | number | `2` | 网络错误时的重试次数 |
| `events` | boolean | `false` | 是否启用 WebSocket 事件（需安装 ws） |
| `debug` | boolean | `false` | 是否开启调试日志（打印完整请求与响应） |

## 在其他插件中使用

本插件作为一个服务（Service）提供，所有封装好的方法都可以通过 `ctx.aria2` 直接调用。

### 1. 声明依赖
```typescript
import { Context } from 'koishi'

export const inject = ['aria2']  // 声明依赖

export function apply(ctx: Context) {
  // 这里可以安全地使用 ctx.aria2
}
```

### 2. 基本调用示例
```typescript
ctx.command('download <url>')
  .action(async ({ session }, url) => {
    const gid = await ctx.aria2.addUri([url], { dir: '/downloads' })
    return `任务已添加，GID: ${gid}`
  })
```

### 3. 监听下载事件
```typescript
ctx.on('aria2/event', (event) => {
  if (event.event === 'complete') {
    // 下载完成后执行操作
  }
})
```
```

### 4. API 方法速查

#### 任务管理
`addUri(uris, options?) : Promise<string>`  
`addTorrent(torrent, uris?, options?) : Promise<string>`  
`pause(gid) : Promise<string>`  
`unpause(gid) : Promise<string>`  
`remove(gid) : Promise<string>`  
`forceRemove(gid) : Promise<string>`  
`changeOption(gid, options) : Promise<string>`

#### 状态查询
`tellStatus(gid, keys?) : Promise<Aria2Status>`  
`tellActive(keys?) : Promise<Aria2Status[]>`  
`tellWaiting(offset?, num?, keys?) : Promise<Aria2Status[]>`  
`tellStopped(offset?, num?, keys?) : Promise<Aria2Status[]>`  
`tellAll(keys?) : Promise<{active, waiting, stopped}>`

#### 全局操作
`getGlobalStat() : Promise<GlobalStat>`  
`purgeDownloadResult() : Promise<string>`  
`shutdown() : Promise<string>`  
`forceShutdown() : Promise<string>`  
`getVersion() : Promise<{version, enabledFeatures}>`

> 所有方法均返回 Promise，并有完整的 TypeScript 类型定义。

## 依赖关系

- **必需（peerDependencies）**：`koishi >= 4.18.7`
- **可选（optional）**：`ws`（当 `events` 开启时需要，用于连接 aria2 的 WebSocket）

## 项目贡献者 (Contributors)

| 贡献者 | 贡献内容 |
|--------|----------|
| Minecraft-1314 | 插件完整开发 |

（欢迎通过 Issues 或 PR 成为贡献者）

## 许可协议 (License)

本项目采用 MIT 许可证，详情参见 [LICENSE](LICENSE) 文件。

This project is licensed under the MIT License, see the [LICENSE](LICENSE) file for details.

## 支持我们 (Support Us)

如果这个项目对您有帮助，欢迎点亮右上角的 Star ⭐ 支持我们，这将是对所有贡献者最大的鼓励！

If this project is helpful to you, please feel free to star it in the upper right corner ⭐ to support us, which will be the greatest encouragement to all contributors!
