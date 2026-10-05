declare module 'koishi' {
  interface Events {
    'opencode/error'(sessionId: string, error: any): void
    'opencode/activity'(sessionId: string): void
    'opencode/status'(sessionId: string, status: string): void
  }
}

import { Context, Schema, h } from 'koishi'
import * as fs from 'fs'
import * as path from 'path'
import { fileURLToPath, pathToFileURL } from 'url'

export const name = 'opencode'

const sleep = (ms: number) => new Promise(resolve => setTimeout(resolve, ms))

// OpenCode 2.x 把 API 挂载在 /api 前缀下，官方 @opencode-ai/sdk 的 v2 客户端即指向这些路径。
// 注意：不再使用 v1 客户端（@opencode-ai/sdk/client），它的根路径 /event、/config/providers
// 在 v2 服务器上会被 Web UI 的 SPA 兜底成 HTML，导致 SSE 立刻结束并无限重连。
async function initializeClients(config: Config): Promise<{ main: any; health: any }> {
  const { createOpencodeClient } = await import('@opencode-ai/sdk/v2/client')

  const headers: Record<string, string> = {}
  if (config.username && config.password) {
    const credentials = Buffer.from(`${config.username}:${config.password}`).toString('base64')
    headers['Authorization'] = `Basic ${credentials}`
  }

  const main = createOpencodeClient({
    baseUrl: config.baseUrl,
    headers,
    ...(config.directory ? { directory: config.directory } : {}),
  })

  return { main, health: main }
}

export interface Config {
  baseUrl: string
  username?: string
  password?: string
  defaultSession?: string
  model?: string
  authority?: number
  timeout?: number
  showReasoning?: boolean
  enableStreaming?: boolean
  streamMode?: 'auto' | 'native' | 'segment'
  streamInterval?: number
  showToolMessages?: boolean
  showProcessingMessage?: boolean
  directory?: string
}



export const Config: Schema<Config> = Schema.intersect([
  Schema.object({
    baseUrl: Schema.string().description('OpenCode Server 地址 (OpenCode 2.x, 无需包含 /api)'),
    username: Schema.string().description('OpenCode Server 用户名 (用于 Basic 认证)'),
    password: Schema.string().role('secret')
      .description('OpenCode Server 密码 (填写后启用 Basic 认证)'),
    defaultSession: Schema.string().description('默认会话 ID'),
    model: Schema.string().description('覆盖默认模型 (格式: provider/model)'),
    timeout: Schema.number().default(30000).description('生成超时时间 (毫秒)'),
    showReasoning: Schema.boolean().description('是否显示 agent 的推理过程').default(false),
    showToolMessages: Schema.boolean().description('是否显示工具调用消息').default(false),
    enableStreaming: Schema.boolean().description('是否开启流式输出').default(false),
    streamMode: Schema.union(['auto', 'native', 'segment']).description('流式输出模式 (auto: 自动检测, native: 编辑消息, segment: 分段发送)').default('auto'),
    streamInterval: Schema.number().description('流式更新间隔 (毫秒)').default(500),
    showProcessingMessage: Schema.boolean().description('是否显示 "正在处理" 提示消息').default(false),
    directory: Schema.string().description('默认工作区目录 (可选)'),
  }).description('OpenCode 连接配置'),
  Schema.object({
    authority: Schema.number().default(1).description('使用命令所需权限等级'),
  }).description('权限配置'),
])

const sessionCache = new Map<string, string>()

// Simple string hash for directory fingerprinting
function simpleHash(str: string): string {
  let hash = 0
  for (let i = 0; i < str.length; i++) {
    const char = str.charCodeAt(i)
    hash = ((hash << 5) - hash) + char
    hash = hash & hash // Convert to 32bit integer
  }
  return Math.abs(hash).toString(36)
}

interface SessionState {
  sessionId: string
  platform: string
  userId: string
  messageId: any
  channelId: string
  guildId?: string
  selfId: string
  opencodeMessageId?: string
  lastActivity?: number
  // cache for message parts: assistantMessageID -> { text?, reasoning? }
  partialMessages?: Map<string, { text?: string; reasoning?: string }>
  // track tool execution phases: callID -> lastStatus
  toolStates?: Map<string, string>
  // remembered tool inputs: callID -> { name, input }
  toolInputs?: Map<string, { name: string; input: any }>

  // Streaming state
  lastStreamTime?: number
  lastStreamMessageId?: string // For native mode
  streamBufferSentIndex?: number // For segment mode: how many chars have been sent
  streamMode?: 'native' | 'segment' | 'auto' // Resolved mode for this session

  // Deduplication tracking
  sentFinalMessages: Set<string> // messageId for step-finish
  sentToolCalls: Set<string> // IDs of sent tool parts
  lastSentContent?: string // To prevent redundant editMessage calls
  streamErrorCount?: number // Continuous failure count for native mode
  isUpdating?: boolean // Background update lock
  hasPendingUpdate?: boolean // Queued update flag
  // Lifecycle
  status?: string // 'idle', 'busy', 'error', etc.
  hasStreamed?: boolean // Whether we have successfully streamed/edited messages
  aborted?: boolean // Set by `oc.stop` to interrupt the in-flight turn
}

const activeSessions = new Map<string, SessionState>()

function findActiveSession(sessionId?: string): { sessionKey: string; sessionState: SessionState } | undefined {
  if (!sessionId) return undefined
  for (const [key, state] of activeSessions.entries()) {
    if (state.sessionId === sessionId) return { sessionKey: key, sessionState: state }
  }
  return undefined
}

function touchSession(sessionId?: string) {
  const found = findActiveSession(sessionId)
  if (found) found.sessionState.lastActivity = Date.now()
}

// refer: https://github.com/anomalyco/opencode/blob/dev/packages/sdk/js/src/gen/types.gen.ts
function formatPart(part: any, showReasoning: boolean = true): string {
  if (!part) return '未知类型'

  switch (part.type) {
    case 'text':
      return part.text || ''

    case 'reasoning':
      if (showReasoning) {
        return `🤔 思考: ${part.text || ''}`
      }
      return ''

    case 'tool':
      const status = part.state?.status
      if (status === 'pending') return '' // Ignore pending

      const tool = part.tool
      const input = part.state?.input || {}
      const output = part.state?.output
      const error = part.state?.error
      const metadata = part.state?.metadata || {}

      let header = ''
      if (status === 'completed') {
        header = `✅ 工具 ${tool} 执行完成`
      } else if (status === 'running') {
        header = `🔧 执行工具: ${tool}`
      } else if (status === 'error') {
        header = `❌ 工具 ${tool} 执行失败`
      } else {
        header = `🔧 工具 ${tool} (${status})`
      }

      let content = ''

      try {
        // Customizable formatting based on tool name
        if (tool === 'todowrite' && Array.isArray(input.todos)) {
          content = '\n' + input.todos.map((t: any) => {
            let mark = '[ ]'
            if (t.status === 'completed') mark = '[x]'
            else if (t.status === 'in_progress') mark = '[/]'
            return `${mark} ${t.content}`
          }).join('\n')
        }
        else if ((tool === 'edit' || tool === 'write' || tool === 'replace_file_content' || tool === 'multi_replace_file_content') && (input.filePath || input.TargetFile)) {
          const file = input.filePath || input.TargetFile || metadata.filepath
          if (file && !header.includes('(')) header += ` (${file})`

          if (metadata.diff) {
            content = `\n\`\`\`diff\n${metadata.diff}\n\`\`\``
          }
        }
        else if (tool === 'shell' || tool === 'bash' || tool === 'run_command') {
          const cmd = input.command || input.CommandLine
          if (cmd) header += `\n$ ${cmd}`

          if (status === 'error' && error) {
            const errStr = typeof error === 'string' ? error : (error?.message || error?.data?.message || JSON.stringify(error))
            content = `\n${errStr}`
          } else if (output) {
            // Heuristic: if command implies diff or output looks like diff/code
            const cmdStr = (cmd || '').trim().toLowerCase()
            if (cmdStr.startsWith('diff') || cmdStr.startsWith('fc') || (typeof output === 'string' && output.includes('diff --git'))) {
              content = `\n\`\`\`diff\n${output}\n\`\`\``
            } else {
              // Limit output length for other commands
              const outStr = String(output)
              content = `\n${outStr.length > 300 ? outStr.substring(0, 300) + '...' : outStr}`
            }
          }
        }
        // Fallback / Generic
        else {
          if (tool === 'webfetch' && input.url) header += ` (${input.url})`
          else if ((tool === 'read' || tool === 'read_file') && (input.filePath || input.path)) header += ` (${input.filePath || input.path})`
          else if (tool === 'skill' && input.name) header += ` (${input.name})`

          if (status === 'error' && error) {
            const errStr = typeof error === 'string' ? error : (error?.message || error?.data?.message || JSON.stringify(error))
            content = `\n${errStr}`
          } else if ((tool === 'read' || tool === 'read_file') && output) {
            const outStr = String(output)
            content = `\n${outStr.length > 300 ? outStr.substring(0, 300) + '...' : outStr}`
          }

          // If we haven't generated valid content yet, try to show something generic if not already in header
          if (!content && !header.includes('(')) {
            const keys = Object.keys(input)
            if (keys.length === 1 && typeof input[keys[0]] === 'string') {
              header += ` (${input[keys[0]]})`
            } else if (keys.length > 0) {
              const inputStr = JSON.stringify(input)
              if (inputStr.length < 100) {
                // Only append if short
                header += ` ${inputStr}`
              }
            }
          }
        }
      } catch (e) {
        // Fallback if parsing fails
      }

      return header + content

    case 'step-start':
      return '' // Don't show step start to user

    case 'step-finish':
      if (part.success) {
        return `✅ 完成步骤: ${part.title || ''}`
      } else {
        return `❌ 失败: ${part.title || ''}`
      }

    case 'agent':
      return `🤖 子代理: ${part.name || '未命名'}`

    case 'subtask':
      return `📋 子任务 (${part.agent}): ${part.description || part.prompt}`

    case 'patch':
      return `📦 补丁 (${part.hash}): ${part.files?.join(', ') || '无文件'}`

    case 'retry':
      const errorMsg = part.error?.data?.message || JSON.stringify(part.error) || ''
      return `🔄 重试 (${part.attempt}次): ${errorMsg}`

    case 'file':
      return `📎 文件: ${part.filename || part.url || '未知文件'}`

    case 'snapshot':
    case 'compaction':
      return '' // Internal types, don't show to user

    default:
      return `📦 ${part.type}`
  }
}

// Convert an OpenCode tool `content` payload into displayable text.
function contentToText(content: any): string {
  if (content == null) return ''
  if (typeof content === 'string') return content
  if (Array.isArray(content)) {
    return content.map((c: any) => (typeof c === 'string' ? c : c?.text)).filter(Boolean).join('\n')
  }
  if (typeof content === 'object') return content.text || JSON.stringify(content)
  return String(content)
}

// Extract the textual output of a v2 projected message.
function extractMessageText(msg: any, showReasoning: boolean): string {
  if (!msg) return ''
  if (msg.type === 'assistant') {
    const parts: string[] = []
    for (const c of msg.content || []) {
      if (c?.type === 'text' && c.text) parts.push(c.text)
      else if (c?.type === 'reasoning' && showReasoning && c.text) parts.push(`🤔 思考: ${c.text}`)
    }
    return parts.join('\n\n')
  }
  if (typeof msg.text === 'string') return msg.text
  return ''
}

export function apply(ctx: Context, config: Config) {
  let client: any = null
  let healthClient: any = null

  ctx.logger.info(`OpenCode 插件正在初始化，连接至: ${config.baseUrl}`)
  ctx.logger.info('showReasoning config:', config.showReasoning)
  const clientPromise = initializeClients(config).then(clients => {
    client = clients.main
    healthClient = clients.health
    ctx.logger.info(`OpenCode 客户端已初始化 (v2)`)
    return clients
  }).catch(err => {
    ctx.logger.error(`OpenCode 客户端初始化失败:`, err)
    throw err
  })

  // Event stream setup
  clientPromise.then(() => {
    setupEventStream(client, ctx, config)
  })


  ctx.command('oc.models [keyword:text]', '列出可用模型', {
    authority: 1,
  })
    .alias('oc.m')
    .usage('列出 OpenCode 服务器支持的所有模型。可以使用关键词过滤。')
    .example('oc.models claude  列出所有包含 claude 的模型')
    .action(async (_, keyword) => {
      try {
        const opencodeClient = await ensureClient()
        const res = await opencodeClient.v2.model.list()
        const models: any[] = res?.data?.data || []

        if (models.length === 0) return '未找到可用模型'

        const kw = keyword ? keyword.toLowerCase() : ''
        const matched = models.filter((m: any) => {
          if (!kw) return true
          return String(m.providerID || '').toLowerCase().includes(kw)
            || String(m.modelID || m.id || '').toLowerCase().includes(kw)
            || String(m.name || '').toLowerCase().includes(kw)
        })

        if (matched.length === 0) return `❌ 未找到包含 "${keyword}" 的模型`

        const grouped = new Map<string, any[]>()
        for (const m of matched) {
          const pid = m.providerID || 'unknown'
          if (!grouped.has(pid)) grouped.set(pid, [])
          grouped.get(pid)!.push(m)
        }

        const providerList = Array.from(grouped.entries()).map(([pid, list]) =>
          `📦 [${pid}]\n` + list.map((m: any) => `  - ${pid}/${m.modelID || m.id}: ${m.name || m.modelID || m.id}`).join('\n')
        )

        return providerList.join('\n\n')
      } catch (error) {
        ctx.logger.error('获取模型列表失败:', error)
        return '❌ 获取模型列表失败'
      }
    })

  ctx.command('oc.model.set <model:string>', '设置默认模型', {
    authority: 3,
  })
    .alias('oc.ms')
    .usage('为当前环境设置默认使用的模型。此设置会影响后续的新会话。')
    .example('oc.ms anthropic/claude-3-5-sonnet  设置为 Claude 3.5 Sonnet')
    .action(async ({ session: chatbotSession }, model) => {
      try {
        if (!model) return '❌ 请提供模型 ID (例如: anthropic/claude-3-5-sonnet)'

        // Simple validation
        if (!model.includes('/')) {
          return '❌ 模型 ID 格式应为 provider/model'
        }

        // Verify validity (optional, but good UX)
        const opencodeClient = await ensureClient()
        const res = await opencodeClient.v2.model.list()
        const models: any[] = res?.data?.data || []
        const isValid = models.some((m: any) => `${m.providerID}/${m.modelID || m.id}` === model || m.modelID === model)

        if (!isValid) {
          return `⚠️ 警告: 未在当前可用列表中找到模型 "${model}"，但仍将强制设置。`
        }

        // Update config
        // ctx.scope.update triggers reload
        await ctx.scope.update((config) => {
          config.model = model
        })

        return `✅ 已将默认模型设置为: ${model} (插件重载中...)`
      } catch (error) {
        ctx.logger.error('设置模型失败:', error)
        return '❌ 设置模型失败'
      }
    })

  const ensureClient = async () => {
    if (!client) {
      await clientPromise
    }
    return client
  }

  const ensureHealthClient = async () => {
    if (!healthClient) {
      await clientPromise
    }
    return healthClient
  }

  ctx.command('oc [...message]', '与 AI 对话')
    .usage('发送消息给 AI 并获取回复。支持上下文记忆和流式输出。')
    .example('oc 你好  发送"你好"')
    .example('oc 给我画一只猫  请求生成图片 (需配置 image 工具)')
    .action(async ({ session: chatbotSession }, ...messageParts) => {
      const message = messageParts.join(' ')
      if (!message.trim()) return // Ignore empty messages

      ctx.logger.info(`[${chatbotSession.platform}-${chatbotSession.userId}] 发送消息: ${message.substring(0, 50)}...`)

      const directory = config.directory || 'default'
      const dirHash = simpleHash(directory)
      const sessionKey = `${chatbotSession.platform}-${chatbotSession.userId}-${dirHash}`

      try {
        const opencodeClient = await ensureClient()
        const opencodeSession = await getOrCreateSession(opencodeClient, chatbotSession, config)

        ctx.logger.info(`[${opencodeSession.id}] 发送消息: ${message.substring(0, 50)}...`)

        const senderName = chatbotSession.username || chatbotSession.author?.name || chatbotSession.userId
        const contextHeader = `[User: ${senderName} (ID: ${chatbotSession.userId}) | Platform: ${chatbotSession.platform}]`

        // Register or merge session state
        const existingState = activeSessions.get(sessionKey)
        const newState: SessionState = {
          ...existingState,
          sessionId: opencodeSession.id,
          platform: chatbotSession.platform,
          userId: chatbotSession.userId,
          messageId: chatbotSession.id,
          channelId: chatbotSession.channelId,
          guildId: chatbotSession.guildId,
          selfId: chatbotSession.selfId,
          lastActivity: Date.now(),
          partialMessages: new Map(),
          toolStates: new Map(),
          toolInputs: new Map(),
          sentFinalMessages: new Set(),
          sentToolCalls: new Set(),
          streamBufferSentIndex: 0,
          lastStreamMessageId: undefined,
          hasStreamed: false,
          status: 'busy',
        }
        activeSessions.set(sessionKey, newState)
        ctx.logger.info(`会话状态已初始化或合并: ${sessionKey}`)

        if (config.showProcessingMessage ?? true) {
          await chatbotSession.send(`🔄 正在处理: ${message.substring(0, 30)}...`)
        }

        const systemInstructions = [
          "如果需要发送媒体文件，在回复中包含标准的 Koishi 元素标签即可自动在用户端渲染对应内容。",
          "图片: <img src='...'/>",
          "音频: <audio src='...'/>",
          "视频: <video src='...'/>",
          "通用文件: <file src='...'/>",
          "请勿对这些标签使用 Markdown 代码块包裹。如果只是发送任务，你不需要读取文件。只需要检查文件是否存在。",
          "如果是本地文件，请使用绝对路径。这是由于你的工作区和文件系统路径可能不同。因此要求你必须使用绝对路径。"
        ].join('\n')

        // OpenCode 2.x 的 prompt 只接受单个 { text } 字段，
        // 因此把系统说明、用户上下文与用户消息合并成一次输入。
        const promptText = [systemInstructions, contextHeader, message].join('\n\n')

        // Apply model override for subsequent turns.
        if (config.model) {
          try {
            await opencodeClient.v2.session.switchModel({
              sessionID: opencodeSession.id,
              model: parseModel(config.model),
            })
          } catch (e) {
            ctx.logger.warn(`[${opencodeSession.id}] 设置模型失败:`, e)
          }
        }

        // Send prompt (v2 uses the low-level client because the generated
        // `session.prompt` signature is out of sync with the server).
        await opencodeClient.client.post({
          url: '/api/session/{sessionID}/prompt',
          path: { sessionID: opencodeSession.id },
          body: { text: promptText },
          headers: { 'Content-Type': 'application/json' },
        })

        const timeout = config.timeout || 30000
        const startTime = Date.now()
        let capturedError: any = null

        try {
          while (true) {
            if (capturedError) break

            const sessionState = activeSessions.get(sessionKey)
            if (!sessionState) {
              // Should not happen with new logic unless manually deleted
              break
            }

            // Interrupted via `oc.stop`: skip the final fallback reply.
            if (sessionState.aborted) {
              capturedError = true
              break
            }

            // Check status from state
            if (sessionState.status === 'idle') {
              break
            }
            if (sessionState.status === 'error') {
              capturedError = true // Mark as error to skip sending
              break
            }

            const lastActivity = sessionState.lastActivity || startTime
            if (Date.now() - lastActivity > timeout) {
              ctx.logger.warn(`[${opencodeSession.id}] 响应生成超时 (无活动 ${timeout}ms)`)
              await chatbotSession.send('⚠️ 响应生成超时')
              capturedError = true // Avoid sending partial result
              break
            }

            await sleep(100)
          }

          // Final check on state
          const finalState = activeSessions.get(sessionKey)
          const hasStreamed = finalState?.hasStreamed ?? false

          if (!capturedError && !hasStreamed) {
            // Only fetch and send if we haven't streamed anything and no error occurred
            const messagesRes = await opencodeClient.v2.session.messages({
              sessionID: opencodeSession.id,
              limit: 50,
            })
            const messages: any[] = messagesRes?.data?.data || []

            // v2 messages are ordered newest-first by default; the newest
            // assistant message corresponds to this turn.
            const assistant = messages.find((m: any) => m.type === 'assistant')
            const formattedResponse = extractMessageText(assistant, config.showReasoning ?? true)

            if (!formattedResponse) {
              await chatbotSession.send('[无响应 - 可能是生成超时或需要更多时间]')
            } else {
              const processedFinal = await processAssets(ctx, formattedResponse)
              await chatbotSession.send(h.parse(processedFinal))
            }
          }

        } finally {
          // Cleanup
          activeSessions.delete(sessionKey)
        }

      } catch (error) {
        const errorMsg = (error as Error).message || String(error)
        ctx.logger.error('OpenCode 错误:', errorMsg)

        activeSessions.delete(sessionKey)

        await chatbotSession.send(`❌ OpenCode 错误: ${errorMsg}`)
      }
    })

  ctx.command('oc.stop', '中断当前对话', {
    authority: config.authority || 1,
  })
    .alias('oc.st')
    .usage('中断当前正在进行中的 AI 对话，并停止 Agent 的后续执行。')
    .example('oc.stop  立即停止当前会话')
    .action(async ({ session: chatbotSession }) => {
      if (!chatbotSession) return

      const directory = config.directory || 'default'
      const dirHash = simpleHash(directory)
      const sessionKey = `${chatbotSession.platform}-${chatbotSession.userId}-${dirHash}`
      const sessionState = activeSessions.get(sessionKey)

      if (!sessionState) {
        return '⚠️ 当前没有正在进行的对话'
      }

      // 先标记中断，让 `oc` 主循环跳过最终回复并清理状态。
      // 直接改对象引用即可，主循环每轮都会重新 get。
      sessionState.aborted = true

      try {
        const opencodeClient = await ensureClient()
        await opencodeClient.v2.session.interrupt({ sessionID: sessionState.sessionId })
        ctx.logger.info(`[${sessionState.sessionId}] 会话已被用户中断`)
        return '🛑 已中断当前对话'
      } catch (error) {
        ctx.logger.error('中断会话失败:', error)
        return '❌ 中断会话失败'
      }
    })

  ctx.command('oc.session.list', '列出活跃会话', {
    authority: 3,
  })
    .alias('oc.sl')
    .usage('显示当前 OpenCode 服务中所有的会话列表。')
    .action(async () => {
      try {
        const opencodeClient = await ensureClient()
        const res = await opencodeClient.v2.session.list()
        const sessions: any[] = res?.data?.data || []

        if (sessions.length === 0) {
          return '暂无会话'
        }

        const list = sessions.map(s =>
          `${s.id}: ${s.title || '未命名'} ${s.model ? `(${s.model.providerID}/${s.model.id})` : ''}`
        ).join('\n')

        return `📋 会话列表:\n${list}`

      } catch (error) {
        ctx.logger.error('列出会话失败:', error)
        return '❌ 列出会话失败'
      }
    })

  ctx.command('oc.session.new', '创建新会话', {
    authority: config.authority || 1,
  })
    .alias('oc.sn')
    .usage('强制创建一个全新的会话。新会话将使用当前配置的工作区目录。')
    .example('oc.sn  创建并切换到新会话')
    .action(async ({ session: chatbotSession }) => {
      try {
        const opencodeClient = await ensureClient()
        const directory = config.directory || 'default'
        const dirHash = simpleHash(directory)
        const newTitle = `Koishi-${chatbotSession.platform}-${chatbotSession.userId}-${dirHash}-${Date.now()}`

        // v2 的 create 支持 title，但生成的 SDK 类型落后于服务器，走低级客户端。
        const newSession = await createOpencodeSession(opencodeClient, config, newTitle)

        const sessionKey = `${chatbotSession.platform}-${chatbotSession.userId}-${dirHash}`
        sessionCache.set(sessionKey, newSession.id)

        return `✅ 已创建会话: ${newSession.id}\n📝 标题: ${newSession.title}\n📂 目录: ${directory}`

      } catch (error) {
        ctx.logger.error('创建会话失败:', error)
        return '❌ 创建会话失败'
      }
    })

  ctx.command('oc.session.set <id:string>', '切换会话', {
    authority: 2,
  })
    .alias('oc.ss')
    .usage('切换到指定的会话 ID。')
    .example('oc.ss 1234-5678  切换到 ID 为 1234-5678 的会话')
    .action(async ({ session: chatbotSession }, id) => {
      try {
        const opencodeClient = await ensureClient()
        const res = await opencodeClient.v2.session.list()
        const sessions: any[] = res?.data?.data || []
        const targetSession = sessions.find(s => s.id === id)

        if (!targetSession) {
          return `❌ 会话 ${id} 不存在`
        }

        const directory = config.directory || 'default'
        const dirHash = simpleHash(directory)
        const sessionKey = `${chatbotSession.platform}-${chatbotSession.userId}-${dirHash}`
        sessionCache.set(sessionKey, id)

        return `✅ 已切换到会话: ${id}\n📝 标题: ${targetSession.title}`

      } catch (error) {
        ctx.logger.error('切换会话失败:', error)
        return '❌ 切换会话失败'
      }
    })

  ctx.command('oc.session.info', '查看当前会话信息', {
    authority: config.authority || 1,
  })
    .alias('oc.si')
    .usage('显示当前连接的会话详情，包括 ID、标题、模型和创建时间。')
    .action(async ({ session: chatbotSession }) => {
      try {
        const opencodeClient = await ensureClient()
        const opencodeSession = await getOrCreateSession(opencodeClient, chatbotSession, config)

        const model = opencodeSession.model
          ? `${opencodeSession.model.providerID}/${opencodeSession.model.id}`
          : '默认'

        return `📌 当前会话信息:\n` +
          `ID: ${opencodeSession.id}\n` +
          `标题: ${opencodeSession.title || '未命名'}\n` +
          `模型: ${model}\n` +
          `创建时间: ${opencodeSession.time?.created ? new Date(opencodeSession.time.created).toLocaleString() : '未知'}`

      } catch (error) {
        ctx.logger.error('获取会话信息失败:', error)
        return '❌ 获取会话信息失败'
      }
    })

  ctx.command('oc.session.delete <id:string>', '删除会话', {
    authority: 4,
  })
    .alias('oc.sdel')
    .usage('删除指定的会话。此操作不可逆。')
    .example('oc.sdel 1234-5678  删除会话')
    .action(async (_, id) => {
      try {
        const opencodeClient = await ensureClient()
        // 生成的 SDK 没有 delete 方法，走低级客户端。
        await opencodeClient.client.delete({
          url: '/api/session/{sessionID}',
          path: { sessionID: id },
        })

        for (const [key, value] of Array.from(sessionCache.entries())) {
          if (value === id) {
            sessionCache.delete(key)
          }
        }

        return `✅ 已删除会话: ${id}`

      } catch (error) {
        ctx.logger.error('删除会话失败:', error)
        return '❌ 删除会话失败'
      }
    })

  ctx.command('oc.health', '检查服务健康状态', {
    authority: config.authority || 1,
  })
    .alias('oc.h')
    .usage('检查 OpenCode 服务器的连接状态和版本信息。')
    .action(async () => {
      try {
        const hc = await ensureHealthClient()
        // v2 没有 /api/health，使用 /api/info 获取版本信息。
        const res = await hc.client.get({ url: '/api/info' })
        const info = res?.data || {}

        return `🏥 OpenCode 状态:\n` +
          `健康: ✅ 正常\n` +
          `版本: ${info.version || '未知'}`

      } catch (error) {
        ctx.logger.error('健康检查失败:', error)
        return '❌ 无法连接到 OpenCode'
      }
    })

  ctx.command('oc.agents', '列出可用 Agents', {
    authority: config.authority || 1,
  })
    .usage('列出 OpenCode 服务器上所有可用的 Agent 工具。')
    .action(async () => {
      try {
        const opencodeClient = await ensureClient()
        const res = await opencodeClient.v2.agent.list()
        const agents: any[] = res?.data?.data || []

        if (!agents || agents.length === 0) {
          return '暂无可用 agents'
        }

        const list = agents.map(a =>
          `🤖 ${a.name || a.id || '未命名'}${a.description ? `\n   ${a.description}` : ''}`
        ).join('\n\n')

        return `📋 可用 Agents:\n\n${list}`

      } catch (error) {
        ctx.logger.error('获取 agents 列表失败:', error)
        return '❌ 获取 agents 列表失败'
      }
    })

  ctx.command('oc.stream.status', '查看流式输出状态', {
    authority: config.authority || 1,
  })
    .usage('查看当前会话的流式输出配置和适配器支持情况。')
    .action(({ session }) => {
      const enable = config.enableStreaming ?? false
      const mode = config.streamMode ?? 'auto'
      const interval = config.streamInterval ?? 500

      let msg = `🌊 流式输出状态:\n`
      msg += `启用: ${enable ? '✅ 开启' : '❌ 关闭'}\n`
      msg += `配置模式: ${mode}\n`

      if (enable) {
        if (mode === 'auto') {
          // Check capability
          const canEdit = session.bot && typeof session.bot.editMessage === 'function'
          msg += `当前判定: ${canEdit ? '⚡ 原生流式 (Native)' : '📝 分段流式 (Segment)'}\n`
          msg += canEdit
            ? `(适配器支持 editMessage)`
            : `(适配器不支持 editMessage，自动降级)`
        } else if (mode === 'native') {
          msg += `当前策略: ⚡ 原生流式 (强制)\n`
          msg += `(注: 若平台不支持，可能会发送失败并回退)`
        } else {
          msg += `当前策略: 📝 分段流式 (强制)`
        }
        msg += `\n更新间隔: ${interval}ms`
      }

      return msg
    })

  ctx.command('oc.session.messages [page:number]', '查看历史消息', {
    authority: config.authority || 1,
  })
    .usage('分页查看当前会话的历史用户消息。')
    .example('oc.session.messages 2  查看第 2 页消息')
    .action(async ({ session: chatbotSession }, page) => {
      try {
        const opencodeClient = await ensureClient()
        const opencodeSession = await getOrCreateSession(opencodeClient, chatbotSession, config)

        const res = await opencodeClient.v2.session.messages({
          sessionID: opencodeSession.id,
          limit: 200,
        })
        const messages: any[] = res?.data?.data || []

        // Filter only user messages, ordered oldest-first.
        const userMessages = messages
          .filter((m: any) => m.type === 'user')
          .sort((a: any, b: any) => (a.time?.created || 0) - (b.time?.created || 0))

        if (userMessages.length === 0) {
          return '暂无用户消息'
        }

        // Pagination setup
        const pageSize = 5
        const totalPages = Math.ceil(userMessages.length / pageSize)
        const currentPage = page || 1

        if (currentPage < 1 || currentPage > totalPages) {
          return `❌ 页码超出范围 (1-${totalPages})`
        }

        // Get messages for current page (newest first)
        const startIndex = (currentPage - 1) * pageSize
        const endIndex = startIndex + pageSize
        const pageMessages = userMessages.slice(startIndex, endIndex).reverse()

        // Format messages
        const formatted = pageMessages.map((m: any, idx: number) => {
          const preview = m.text || '[无文本]'
          return `${startIndex + idx + 1}. ${preview.substring(0, 100)}${preview.length > 100 ? '...' : ''}`
        }).join('\n')

        return `📜 消息历史 (第 ${currentPage}/${totalPages} 页):\n\n${formatted}\n\n💡 使用 "oc.session.messages ${currentPage + 1}" 查看下一页`
      } catch (error) {
        ctx.logger.error('获取消息历史失败:', error)
        return '❌ 获取消息历史失败'
      }
    })


}

// Extract a useful message from a hey-api result so that failures like
// "Failed to create session" also report *why* (HTTP status / server error).
function describeApiError(result: any, fallback: string): string {
  const status = result?.response?.status
  const err = result?.error
  let detail = ''
  if (typeof err === 'string') {
    detail = err
  } else if (err) {
    detail = err.message || err.data?.message || err._tag || JSON.stringify(err)
  }
  const suffix = [status ? `HTTP ${status}` : '', detail].filter(Boolean).join(', ')
  return suffix ? `${fallback} (${suffix})` : fallback
}

// Create a session. The low-level endpoint is preferred because it is the only
// one that forwards `title` (the generated `v2.session.create` drops it), but
// SDK builds differ in how the low-level client is exposed, so fall back to the
// typed method when the primary call yields no session.
async function createOpencodeSession(client: any, config: Config, title: string): Promise<any> {
  const body: any = { title }
  if (config.directory) body.location = { directory: config.directory }
  if (config.model) body.model = parseModel(config.model)

  let result: any
  try {
    result = await client.client.post({
      url: '/api/session',
      body,
      headers: { 'Content-Type': 'application/json' },
    })
  } catch (error) {
    result = { error: (error as Error).message }
  }

  // The server wraps the session as `{ data: SessionV2Info }`; tolerate
  // versions that return it directly.
  const created = result?.data?.data?.id ? result.data.data : (result?.data?.id ? result.data : null)
  if (created?.id) return created

  // Fallback for SDK builds whose low-level client is missing or changed.
  // The typed create does not forward `title`, so the session is untitled; the
  // in-memory cache still reuses it for the lifetime of this process.
  try {
    const typed = await client.v2.session.create({
      ...(config.directory ? { directory: config.directory } : {}),
      ...(config.model ? { model: parseModel(config.model) } : {}),
    })
    const session = typed?.data?.data
    if (session?.id) return session
  } catch { /* fall through to the descriptive error */ }

  throw new Error(describeApiError(result, 'Failed to create session'))
}

async function getOrCreateSession(
  client: any,
  chatbotSession: any,
  config: Config,
): Promise<any> {
  const directory = config.directory || 'default'
  const dirHash = simpleHash(directory)
  const sessionKey = `${chatbotSession.platform}-${chatbotSession.userId}-${dirHash}`
  const cachedId = sessionCache.get(sessionKey)

  // 1. Try cache
  if (cachedId) {
    try {
      const res = await client.v2.session.get({ sessionID: cachedId })
      const data = res?.data?.data
      if (data && data.id) {
        return data
      }
      // If cache invalid (404), remove and continue
      sessionCache.delete(sessionKey)
    } catch {
      sessionCache.delete(sessionKey)
    }
  }

  // 2. Try to find existing session by Title keys
  const titlePrefix = `Koishi-${chatbotSession.platform}-${chatbotSession.userId}-${dirHash}`
  try {
    const res = await client.v2.session.list({ limit: 200 })
    const sessions: any[] = res?.data?.data || []
    if (sessions.length > 0) {
      const candidates = sessions.filter((s: any) => s.title && s.title.startsWith(titlePrefix))

      if (candidates.length > 0) {
        // Sort by creation time, newest first
        candidates.sort((a: any, b: any) => {
          return (b.time?.created || 0) - (a.time?.created || 0)
        })

        const best = candidates[0]
        sessionCache.set(sessionKey, best.id)
        return best
      }
    }
  } catch (error) {
    // List failed?
  }

  // 3. Create new session
  const newTitle = `${titlePrefix}-${Date.now()}`
  const created = await createOpencodeSession(client, config, newTitle)
  sessionCache.set(sessionKey, created.id)
  return created
}

function parseModel(modelStr: string): { id: string; providerID: string } {
  const parts = modelStr.split('/')
  if (parts.length !== 2) {
    throw new Error(`模型格式错误，应为: provider/model (例如: anthropic/claude-3-5-sonnet-20241022)`)
  }
  return { providerID: parts[0], id: parts[1] }
}

let isSubscribed = false

async function setupEventStream(client: any, ctx: Context, config: Config) {
  if (isSubscribed) return
  isSubscribed = true

  ctx.on('dispose', () => {
    isSubscribed = false
  })

  while (isSubscribed) {
    try {
      ctx.logger.info('正在启用 OpenCode 事件流订阅...')
      const events = await client.v2.event.subscribe()

      for await (const event of events.stream) {
        if (!isSubscribed) break
        await handleEvent(ctx, event, config)
      }

      if (isSubscribed) {
        ctx.logger.warn('OpenCode 事件流已断开，2 秒后重连...')
        await sleep(2000)
      }
    } catch (error) {
      if (isSubscribed) {
        ctx.logger.warn('OpenCode 事件流连接中断，5秒后尝试重连...', error)
        await sleep(5000)
      }
    }
  }

  ctx.logger.info('OpenCode 事件流订阅已停止')
}

async function handleEvent(ctx: Context, event: any, config: Config) {
  const data = event?.data || {}
  if (data.sessionID) touchSession(data.sessionID)

  switch (event.type) {
    case 'session.execution.started':
      break
    case 'session.execution.succeeded':
    case 'session.execution.failed':
      await handleExecutionEnd(ctx, event)
      break
    case 'session.status':
      await handleSessionStatus(ctx, event)
      break
    case 'session.error':
      await handleSessionError(ctx, event)
      break
    case 'session.text.delta':
      await handleDelta(ctx, event, config, 'text')
      break
    case 'session.reasoning.delta':
      await handleDelta(ctx, event, config, 'reasoning')
      break
    case 'session.text.ended':
      await handleDeltaEnded(ctx, event, config, 'text')
      break
    case 'session.reasoning.ended':
      await handleDeltaEnded(ctx, event, config, 'reasoning')
      break
    case 'session.step.ended':
      await handleStepEnded(ctx, event, config)
      break
    case 'session.tool.input.started':
    case 'session.tool.called':
    case 'session.tool.success':
    case 'session.tool.failed':
      await handleToolEvent(ctx, event, config)
      break
    case 'session.created':
      if (data.sessionID) ctx.logger.info(`会话创建: ${data.sessionID}`)
      break
    case 'session.deleted':
      if (data.sessionID) ctx.logger.info(`会话删除: ${data.sessionID}`)
      break
    default:
      ctx.logger.debug(`OpenCode 事件 [${event.type}]`)
  }
}

async function handleDelta(ctx: Context, event: any, config: Config, kind: 'text' | 'reasoning') {
  const data = event.data || {}
  const found = findActiveSession(data.sessionID)
  if (!found) return

  const { sessionKey, sessionState } = found
  const messageId = data.assistantMessageID
  if (!messageId) return

  if (!sessionState.partialMessages) sessionState.partialMessages = new Map()
  const current = sessionState.partialMessages.get(messageId) || {}
  const delta = data.delta || ''

  if (kind === 'text') current.text = (current.text || '') + delta
  else current.reasoning = (current.reasoning || '') + delta

  sessionState.partialMessages.set(messageId, current)
  sessionState.lastActivity = Date.now()
  activeSessions.set(sessionKey, sessionState)

  await maybeStream(ctx, sessionState, sessionKey, messageId, config, false)
}

async function handleDeltaEnded(ctx: Context, event: any, config: Config, kind: 'text' | 'reasoning') {
  const data = event.data || {}
  const found = findActiveSession(data.sessionID)
  if (!found) return

  const { sessionKey, sessionState } = found
  const messageId = data.assistantMessageID
  if (!messageId) return

  if (!sessionState.partialMessages) sessionState.partialMessages = new Map()
  const current = sessionState.partialMessages.get(messageId) || {}
  if (typeof data.text === 'string') {
    if (kind === 'text') current.text = data.text
    else current.reasoning = data.text
    sessionState.partialMessages.set(messageId, current)
  }

  sessionState.lastActivity = Date.now()
  activeSessions.set(sessionKey, sessionState)

  await maybeStream(ctx, sessionState, sessionKey, messageId, config, false)
}

async function handleStepEnded(ctx: Context, event: any, config: Config) {
  const data = event.data || {}
  const found = findActiveSession(data.sessionID)
  if (!found) return

  const { sessionKey, sessionState } = found
  sessionState.lastActivity = Date.now()

  await maybeStream(ctx, sessionState, sessionKey, data.assistantMessageID, config, true)
  activeSessions.set(sessionKey, sessionState)
}

// Build the accumulated content for a message and run the configured streaming mode.
async function maybeStream(
  ctx: Context,
  sessionState: SessionState,
  sessionKey: string,
  messageId: string | undefined,
  config: Config,
  isStepFinish: boolean,
) {
  const enableStreaming = config.enableStreaming ?? false
  const showReasoning = config.showReasoning ?? true
  const streamInterval = config.streamInterval ?? 500

  const current = messageId ? sessionState.partialMessages?.get(messageId) : undefined
  const parts: string[] = []
  if (showReasoning && current?.reasoning) parts.push(`🤔 思考: ${current.reasoning}`)
  if (current?.text) parts.push(current.text)
  const fullContent = parts.join('\n\n')

  if (!enableStreaming) {
    // Non-streaming: deliver the accumulated answer when the step finishes.
    if (isStepFinish) await sendStepContent(ctx, sessionState, sessionKey, messageId, fullContent)
    return
  }
  if (!fullContent && !isStepFinish) return

  // Determine streaming mode if not set
  if (!sessionState.streamMode) {
    const streamModeConfig = config.streamMode ?? 'auto'
    if (streamModeConfig === 'native') {
      sessionState.streamMode = 'native'
    } else if (streamModeConfig === 'segment') {
      sessionState.streamMode = 'segment'
    } else {
      const bot = ctx.bots.find(b => b.platform === sessionState.platform && b.selfId === sessionState.selfId)
      sessionState.streamMode = (bot && typeof bot.editMessage === 'function') ? 'native' : 'segment'
    }
  }

  if (sessionState.streamMode === 'native') {
    await handleNativeStreaming(ctx, sessionState, messageId, streamInterval, isStepFinish, showReasoning)
  } else {
    await handleSegmentedStreaming(ctx, sessionState, fullContent, isStepFinish)
  }

  if (isStepFinish && (sessionState.streamMode === 'native' || sessionState.streamMode === 'segment')) {
    sessionState.hasStreamed = true
  }

  activeSessions.set(sessionKey, sessionState)
}

// Send the final accumulated content for a step when streaming is disabled.
async function sendStepContent(
  ctx: Context,
  sessionState: SessionState,
  sessionKey: string,
  messageId: string | undefined,
  fullContent: string,
) {
  if (!fullContent) return

  const dedupKey = `final-text:${messageId || 'unknown'}`
  if (sessionState.sentFinalMessages.has(dedupKey)) return

  const bot = ctx.bots.find(b => b.platform === sessionState.platform && b.selfId === sessionState.selfId)
  if (!bot) {
    ctx.logger.warn(`Bot not found for session ${sessionKey}`)
    return
  }

  const processed = await processAssets(ctx, fullContent)
  await bot.sendMessage(sessionState.channelId, h.parse(processed), sessionState.guildId)
  sessionState.sentFinalMessages.add(dedupKey)
  sessionState.hasStreamed = true
  activeSessions.set(sessionKey, sessionState)
}

async function handleNativeStreaming(
  ctx: Context,
  sessionState: SessionState,
  messageId: string | undefined,
  streamInterval: number,
  isStepFinish: boolean,
  showReasoning: boolean,
): Promise<void> {
  // 1. Check if already updating
  if (sessionState.isUpdating) {
    sessionState.hasPendingUpdate = true
    return
  }

  const now = Date.now()
  const lastTime = sessionState.lastStreamTime || 0

  // 2. Initial trigger check: Don't skip if message hasn't been created yet
  if (!isStepFinish && (now - lastTime < streamInterval) && sessionState.lastStreamMessageId) {
    return
  }

  sessionState.isUpdating = true

  try {
    // 3. Update Loop
    while (true) {
      sessionState.hasPendingUpdate = false

      const current = messageId ? sessionState.partialMessages?.get(messageId) : undefined
      if (!current) break

      const parts: string[] = []
      if (showReasoning && current.reasoning) {
        parts.push(`🤔 思考: ${current.reasoning}`)
      }
      if (current.text) {
        parts.push(current.text)
      }
      const latestFullContent = parts.join('\n\n')

      if (!latestFullContent) break

      // Tag Integrity & Standalone Media Detection
      const incompleteTagRegex = /<[a-zA-Z][^>]*$/
      const standaloneMediaRegex = /^\s*<(img|audio|video|file)[^>]*\/>\s*$/i

      const isStandaloneMedia = !current.reasoning && standaloneMediaRegex.test(latestFullContent.trim())
      const isTagIncomplete = incompleteTagRegex.test(latestFullContent)

      // Skip if tag is cut off, unless it's the very end
      if (isTagIncomplete && !isStepFinish) {
        sessionState.hasPendingUpdate = true
        break
      }

      const bot = ctx.bots.find(b => b.platform === sessionState?.platform && b.selfId === sessionState?.selfId)
      if (!bot) break

      try {
        const processedContent = await processAssets(ctx, latestFullContent)

        if (isStandaloneMedia) {
          // Case A: Standalone Media - Send as new message to ensure platform rendering
          try {
            await bot.sendMessage(sessionState.channelId, h.parse(processedContent), sessionState.guildId)
            sessionState.lastSentContent = processedContent
            sessionState.lastStreamTime = Date.now()
            sessionState.hasStreamed = true
            sessionState.streamErrorCount = 0
            sessionState.lastStreamMessageId = undefined // Cut off for subsequent text
            if (!isStepFinish) break
          } catch (sendErr) {
            ctx.logger.error(`Failed to send standalone media:`, sendErr)
            break
          }
        } else {
          // Case B: Normal Edit or First Message
          if (processedContent === sessionState.lastSentContent && !isStepFinish) {
            // Deduplication
          } else if (sessionState.lastStreamMessageId) {
            try {
              await bot.editMessage(sessionState.channelId, sessionState.lastStreamMessageId, h.parse(processedContent))
              sessionState.lastSentContent = processedContent
              sessionState.lastStreamTime = Date.now()
              sessionState.hasStreamed = true
              sessionState.streamErrorCount = 0
            } catch (editErr) {
              sessionState.streamErrorCount = (sessionState.streamErrorCount || 0) + 1
              if (isStepFinish) {
                ctx.logger.warn(`Final edit failed, falling back to sendMessage:`, editErr)
                await bot.sendMessage(sessionState.channelId, h.parse(processedContent), sessionState.guildId)
                sessionState.hasStreamed = true
              } else if (sessionState.streamErrorCount >= 3) {
                throw editErr // Trigger mode downgrade
              }
            }
          } else {
            const sentIds = await bot.sendMessage(sessionState.channelId, h.parse(processedContent), sessionState.guildId)
            if (sentIds && sentIds.length > 0) {
              sessionState.lastStreamMessageId = sentIds[0]
              sessionState.lastSentContent = processedContent
              sessionState.lastStreamTime = Date.now()
              sessionState.hasStreamed = true
              sessionState.streamErrorCount = 0
            }
          }
        }
      } catch (err) {
        ctx.logger.warn(`Native streaming failed, downgrading to segment mode:`, err)
        sessionState.streamMode = 'segment'
        sessionState.streamBufferSentIndex = sessionState.lastSentContent?.length || 0
        break
      }

      if (!sessionState.hasPendingUpdate && !isStepFinish) break
      if (isStepFinish && !sessionState.hasPendingUpdate) break
      await sleep(100)
    }
  } finally {
    sessionState.isUpdating = false
    if (isStepFinish) {
      sessionState.hasStreamed = true
    }
  }
}

async function processAssets(ctx: Context, content: string): Promise<string> {
  const elements = h.parse(content)
  let changed = false

  for (const element of elements) {
    if (['img', 'audio', 'video', 'file'].includes(element.type)) {
      const src = element.attrs.src
      if (src && (path.isAbsolute(src) || src.startsWith('file://'))) {
        try {
          const localPath = src.startsWith('file://') ? fileURLToPath(src) : src
          if (fs.existsSync(localPath)) {
            const fileName = path.basename(localPath)
            const targetDir = path.resolve(ctx.baseDir, 'data/opencode/temp')
            if (!fs.existsSync(targetDir)) {
              fs.mkdirSync(targetDir, { recursive: true })
            }
            const targetPath = path.join(targetDir, fileName)
            fs.copyFileSync(localPath, targetPath)

            // Decode URI to show Chinese characters correctly in the message/log
            const fileUrl = decodeURI(pathToFileURL(targetPath).href)
            element.attrs.src = fileUrl
            ctx.logger.info(`[Asset] 资源已处理: ${localPath} -> ${fileUrl}`)
            changed = true
          } else {
            ctx.logger.info(`[Asset] 本地文件不存在，跳过: ${localPath}`)
          }
        } catch (err) {
          ctx.logger.error(`[Asset] 处理资源失败: ${src}`, err)
        }
      }
    }
  }

  return changed ? elements.join('') : content
}

async function handleSegmentedStreaming(
  ctx: Context,
  sessionState: SessionState,
  fullContent: string,
  isStepFinish: boolean
): Promise<boolean> {
  const sentIndex = sessionState.streamBufferSentIndex || 0
  const newContent = fullContent.substring(sentIndex)
  let handled = false

  if (newContent) {
    let toSend = ''
    let newSentIndex = sentIndex

    if (isStepFinish) {
      toSend = newContent
      newSentIndex = fullContent.length
    } else {
      // Check for sentence boundaries
      // Find safe split index
      let splitIndex = -1

      // Stricter delimiters: \n\n, 。, . (followed by space or end of string)
      const d1 = newContent.lastIndexOf('\n\n')
      const d2 = newContent.lastIndexOf('。')
      // Check for ". " (dot preceded by a letter and followed by whitespace)
      let d3 = -1
      const dotMatch = [...newContent.matchAll(/[a-zA-Z]\.[\s\n]/g)]
      if (dotMatch.length > 0) {
        // match index is for the character, dot is +1
        d3 = dotMatch[dotMatch.length - 1].index! + 1
      }

      const lastDelimiter = Math.max(d1 !== -1 ? d1 + 1 : -1, d2, d3)
      // Adjust splitIndex based on delimiter (include delimiter for 。 and . )
      if (lastDelimiter !== -1) {
        if (lastDelimiter === d1 + 1) splitIndex = d1 + 2 // After \n\n
        else splitIndex = lastDelimiter + 1
      }

      // Tag Integrity Protection
      // Check if the potential chunk (or the whole newContent if no delimiter) ends with an incomplete media tag
      const candidate = splitIndex !== -1 ? newContent.substring(0, splitIndex) : newContent

      // Regex to match incomplete tags at the end of string: <(image|audio|video|file)... without closing >
      // We look for: < followed by one of the keywords, optionally more content, but NOT followed by >
      const incompleteTagRegex = /<(?:img|audio|video|file)[^>]*$/i

      if (incompleteTagRegex.test(candidate)) {
        // Tag integrity logic...
        const match = candidate.match(/<(?:img|audio|video|file)[^>]*$/i)
        if (match) {
          const tagStart = match.index!
          if (splitIndex !== -1 && splitIndex <= tagStart) {
            // Already splitting before the tag
          } else {
            if (tagStart > 0) splitIndex = tagStart
            else splitIndex = -1
          }
        }
      }

      if (splitIndex !== -1) {
        toSend = newContent.substring(0, splitIndex)
        newSentIndex = sentIndex + toSend.length
      }
    }

    if (toSend) {
      const bot = ctx.bots.find(b => b.platform === sessionState?.platform && b.selfId === sessionState?.selfId)
      if (bot) {
        const processedToSend = await processAssets(ctx, toSend)
        await bot.sendMessage(sessionState.channelId, h.parse(processedToSend), sessionState.guildId)
        sessionState.streamBufferSentIndex = newSentIndex
        sessionState.hasStreamed = true
      }
    }
  }

  if (isStepFinish) {
    sessionState.hasStreamed = true
    handled = true
  }
  return handled
}

async function handleExecutionEnd(ctx: Context, event: any) {
  const data = event.data || {}
  const sessionId = data.sessionID
  const found = findActiveSession(sessionId)
  if (!found) return

  const { sessionKey, sessionState } = found
  sessionState.lastActivity = Date.now()
  sessionState.status = event.type === 'session.execution.failed' ? 'error' : 'idle'
  activeSessions.set(sessionKey, sessionState)
  ctx.logger.info(`Session ${sessionId} ${sessionState.status}`)
}

async function handleToolEvent(ctx: Context, event: any, config: Config) {
  if (!(config.showToolMessages ?? true)) return

  const data = event.data || {}
  const found = findActiveSession(data.sessionID)
  if (!found) return

  const { sessionKey, sessionState } = found
  if (!sessionState.toolStates) sessionState.toolStates = new Map()
  if (!sessionState.toolInputs) sessionState.toolInputs = new Map()

  const callId = data.id || data.callID || 'unknown'
  const remembered = sessionState.toolInputs.get(callId)
  const name = data.name || remembered?.name || 'tool'
  const input = data.input || remembered?.input || {}
  sessionState.toolInputs.set(callId, { name, input })

  let part: any = null
  if (event.type === 'session.tool.called') {
    part = { type: 'tool', tool: name, callID: callId, id: callId, state: { status: 'running', input } }
  } else if (event.type === 'session.tool.success') {
    part = { type: 'tool', tool: name, callID: callId, id: callId, state: { status: 'completed', input, output: contentToText(data.content), metadata: data.metadata || {} } }
  } else if (event.type === 'session.tool.failed') {
    part = { type: 'tool', tool: name, callID: callId, id: callId, state: { status: 'error', input, error: data.error || data.message || '执行失败', metadata: data.metadata || {} } }
  } else {
    // session.tool.input.started / session.tool.progress: nothing to show yet
    return
  }

  const status = part.state.status
  const dedupKey = `${callId}:${status}`
  if (sessionState.sentToolCalls.has(dedupKey)) return

  const text = formatPart(part, config.showReasoning ?? true)
  if (!text) return

  const bot = ctx.bots.find(b => b.platform === sessionState.platform && b.selfId === sessionState.selfId)
  if (!bot) {
    ctx.logger.warn(`Bot not found for session ${sessionKey}`)
    return
  }

  const processed = await processAssets(ctx, text)
  await bot.sendMessage(sessionState.channelId, h.parse(processed), sessionState.guildId)
  sessionState.sentToolCalls.add(dedupKey)
  sessionState.hasStreamed = true
  sessionState.lastActivity = Date.now()
  activeSessions.set(sessionKey, sessionState)
}

async function handleSessionStatus(ctx: Context, event: any) {
  const data = event.data || {}
  const status = data.status?.type || data.status || 'unknown'
  const sessionId = data.sessionID

  const found = findActiveSession(sessionId)
  if (!found) {
    ctx.logger.warn(`No active session found for status update: ${sessionId}`)
    return
  }

  const { sessionKey, sessionState } = found
  sessionState.lastActivity = Date.now()

  if (status === 'idle') {
    sessionState.status = 'idle'
  } else {
    sessionState.status = status
  }
  activeSessions.set(sessionKey, sessionState)
  ctx.logger.info(`Session ${sessionId} status: ${status}`)
}

async function handleSessionError(ctx: Context, event: any) {
  const data = event.data || {}
  const sessionId = data.sessionID

  const found = findActiveSession(sessionId)
  if (!found) {
    ctx.logger.warn(`No active session found for error: ${sessionId}`)
    return
  }

  const { sessionKey, sessionState } = found
  const errData = data.error
  const errMessage = errData?.message || errData?.data?.message || JSON.stringify(errData || {})

  // Mark session as error for main loop
  sessionState.status = 'error'
  activeSessions.set(sessionKey, sessionState)

  const bot = ctx.bots.find(b => b.platform === sessionState?.platform && b.selfId === sessionState?.selfId)
  if (bot) {
    await bot.sendMessage(sessionState.channelId, `❌ 会话错误: ${errMessage}`, sessionState.guildId)
  }

  ctx.logger.error(`Session ${sessionId} error: ${errMessage}`)
}
